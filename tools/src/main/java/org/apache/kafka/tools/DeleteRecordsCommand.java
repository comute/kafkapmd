/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.Json;
import org.apache.kafka.server.util.json.DecodeJson;
import org.apache.kafka.server.util.json.JsonObject;
import org.apache.kafka.server.util.json.JsonValue;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * A command for delete records of the given partitions down to the specified offset.
 */
public class DeleteRecordsCommand {
    private static final int EARLIEST_VERSION = 1;

    private static final DecodeJson.DecodeInteger INT = new DecodeJson.DecodeInteger();

    private static final DecodeJson.DecodeLong LONG = new DecodeJson.DecodeLong();

    private static final DecodeJson.DecodeString STRING = new DecodeJson.DecodeString();

    public static void main(String[] args) throws Exception {
        execute(args, System.out);
    }

    static Collection<Tuple<TopicPartition, Long>> parseOffsetJsonStringWithoutDedup(String jsonData) throws JsonProcessingException {
        JsonValue js = Json.parseFull(jsonData)
            .orElseThrow(() -> new AdminOperationException("The input string is not a valid JSON"));

        Optional<JsonValue> version = js.asJsonObject().get("version");

        return parseJsonData(version.isPresent() ? version.get().to(INT) : EARLIEST_VERSION, js);
    }

    private static Collection<Tuple<TopicPartition, Long>> parseJsonData(int version, JsonValue js) throws JsonMappingException {
        if (version == 1) {
            JsonValue partitions = js.asJsonObject().get("partitions")
                .orElseThrow(() -> new AdminOperationException("Missing partitions field"));

            Collection<Tuple<TopicPartition, Long>> res = new ArrayList<>();

            Iterator<JsonValue> iterator = partitions.asJsonArray().iterator();

            while (iterator.hasNext()) {
                JsonObject partitionJs = iterator.next().asJsonObject();

                String topic = partitionJs.apply("topic").to(STRING);
                int partition = partitionJs.apply("partition").to(INT);
                long offset = partitionJs.apply("offset").to(LONG);

                res.add(new Tuple<>(new TopicPartition(topic, partition), offset));
            }

            return res;
        }

        throw new AdminOperationException("Not supported version field value " + version);
    }

    public static void execute(String[] args, PrintStream out) throws IOException {
        DeleteRecordsCommandOptions opts = new DeleteRecordsCommandOptions(args);

        try (Admin adminClient = createAdminClient(opts)) {
            execute(adminClient, Utils.readFileAsString(opts.options.valueOf(opts.offsetJsonFileOpt)), out);
        }
    }

    static void execute(Admin adminClient, String offsetJsonString, PrintStream out) throws JsonProcessingException {
        Collection<Tuple<TopicPartition, Long>> offsetSeq = parseOffsetJsonStringWithoutDedup(offsetJsonString);

        Set<TopicPartition> duplicatePartitions =
            ToolsUtils.duplicates(offsetSeq.stream().map(Tuple::v1).collect(Collectors.toList()));

        if (!duplicatePartitions.isEmpty()) {
            StringJoiner duplicates = new StringJoiner(",");
            duplicatePartitions.forEach(tp -> duplicates.add(tp.toString()));
            throw new AdminCommandFailedException(
                String.format("Offset json file contains duplicate topic partitions: %s", duplicates)
            );
        }

        Map<TopicPartition, RecordsToDelete> recordsToDelete = offsetSeq.stream()
            .map(tuple -> new Tuple<>(tuple.v1, RecordsToDelete.beforeOffset(tuple.v2)))
            .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        out.println("Executing records delete operation");
        DeleteRecordsResult deleteRecordsResult = adminClient.deleteRecords(recordsToDelete);
        out.println("Records delete operation completed:");

        deleteRecordsResult.lowWatermarks().forEach((tp, partitionResult) -> {
            try {
                out.printf("partition: %s\tlow_watermark: %s%n", tp, partitionResult.get().lowWatermark());
            } catch (InterruptedException | ExecutionException e) {
                out.printf("partition: %s\terror: %s%n", tp, e.getMessage());
            }
        });
    }

    private static Admin createAdminClient(DeleteRecordsCommandOptions opts) throws IOException {
        Properties props = opts.options.has(opts.commandConfigOpt)
            ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
            : new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
        return Admin.create(props);
    }

    private static class DeleteRecordsCommandOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> offsetJsonFileOpt;
        private final OptionSpec<String> commandConfigOpt;

        public DeleteRecordsCommandOptions(String[] args) {
            super(args);

            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server to connect to.")
                .withRequiredArg()
                .describedAs("server(s) to use for bootstrapping")
                .ofType(String.class);

            offsetJsonFileOpt = parser.accepts("offset-json-file", "REQUIRED: The JSON file with offset per partition. " +
                    "The format to use is:\n" +
                    "{\"partitions\":\n  [{\"topic\": \"foo\", \"partition\": 1, \"offset\": 1}],\n \"version\":1\n}")
                .withRequiredArg()
                .describedAs("Offset json file path")
                .ofType(String.class);

            commandConfigOpt = parser.accepts("command-config", "A property file containing configs to be passed to Admin Client.")
                .withRequiredArg()
                .describedAs("command config property file path")
                .ofType(String.class);

            options = parser.parse(args);

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to delete records of the given partitions down to the specified offset.");

            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, offsetJsonFileOpt);
        }
    }

    public static final class Tuple<V1, V2> {
        private final V1 v1;

        private final V2 v2;

        public Tuple(V1 v1, V2 v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        public V1 v1() {
            return v1;
        }

        public V2 v2() {
            return v2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple<?, ?> tuple = (Tuple<?, ?>) o;
            return Objects.equals(v1, tuple.v1) && Objects.equals(v2, tuple.v2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(v1, v2);
        }

        @Override
        public String toString() {
            return "Tuple{v1=" + v1 + ", v2=" + v2 + '}';
        }
    }
}
