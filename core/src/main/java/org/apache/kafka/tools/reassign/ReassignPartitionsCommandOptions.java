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

package org.apache.kafka.tools.reassign;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpecBuilder;
import org.apache.kafka.server.util.CommandDefaultOptions;

public class ReassignPartitionsCommandOptions extends CommandDefaultOptions {
    // Actions
    private final OptionSpecBuilder verifyOpt;
    private final OptionSpecBuilder generateOpt;
    private final OptionSpecBuilder executeOpt;
    private final OptionSpecBuilder cancelOpt;
    private final OptionSpecBuilder listOpt;

    // Arguments
    private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
    private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
    private final ArgumentAcceptingOptionSpec<String> reassignmentJsonFileOpt;
    private final ArgumentAcceptingOptionSpec<String> topicsToMoveJsonFileOpt;
    private final ArgumentAcceptingOptionSpec<String> brokerListOpt;
    private final OptionSpecBuilder disableRackAware;
    private final ArgumentAcceptingOptionSpec<Long> interBrokerThrottleOpt;
    private final ArgumentAcceptingOptionSpec<Long> replicaAlterLogDirsThrottleOpt;
    private final ArgumentAcceptingOptionSpec<Long> timeoutOpt;
    private final OptionSpecBuilder additionalOpt;
    private final OptionSpecBuilder preserveThrottlesOpt;

    public ReassignPartitionsCommandOptions(String[] args) {
        super(args);

        verifyOpt = parser.accepts("verify", "Verify if the reassignment completed as specified by the " +
            "--reassignment-json-file option. If there is a throttle engaged for the replicas specified, and the rebalance has completed, the throttle will be removed");
        generateOpt = parser.accepts("generate", "Generate a candidate partition reassignment configuration." +
            " Note that this only generates a candidate assignment, it does not execute it.");
        executeOpt = parser.accepts("execute", "Kick off the reassignment as specified by the --reassignment-json-file option.");
        cancelOpt = parser.accepts("cancel", "Cancel an active reassignment.");
        listOpt = parser.accepts("list", "List all active partition reassignments.");

        // Arguments
        bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: the server(s) to use for bootstrapping.")
            .withRequiredArg()
            .describedAs("Server(s) to use for bootstrapping")
            .ofType(String.class);

        commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
            .withRequiredArg()
            .describedAs("Admin client property file")
            .ofType(String.class);

        reassignmentJsonFileOpt = parser.accepts("reassignment-json-file", "The JSON file with the partition reassignment configuration" +
                "The format to use is - \n" +
                "{\"partitions\":\n\t[{\"topic\": \"foo\",\n\t  \"partition\": 1,\n\t  \"replicas\": [1,2,3],\n\t  \"log_dirs\": [\"dir1\",\"dir2\",\"dir3\"] }],\n\"version\":1\n}\n" +
                "Note that \"log_dirs\" is optional. When it is specified, its length must equal the length of the replicas list. The value in this list " +
                "can be either \"any\" or the absolution path of the log directory on the broker. If absolute log directory path is specified, the replica will be moved to the specified log directory on the broker.")
            .withRequiredArg()
            .describedAs("manual assignment json file path")
            .ofType(String.class);
        topicsToMoveJsonFileOpt = parser.accepts("topics-to-move-json-file", "Generate a reassignment configuration to move the partitions" +
                " of the specified topics to the list of brokers specified by the --broker-list option. The format to use is - \n" +
                "{\"topics\":\n\t[{\"topic\": \"foo\"},{\"topic\": \"foo1\"}],\n\"version\":1\n}")
            .withRequiredArg()
            .describedAs("topics to reassign json file path")
            .ofType(String.class);
        brokerListOpt = parser.accepts("broker-list", "The list of brokers to which the partitions need to be reassigned" +
                " in the form \"0,1,2\". This is required if --topics-to-move-json-file is used to generate reassignment configuration")
            .withRequiredArg()
            .describedAs("brokerlist")
            .ofType(String.class);
        disableRackAware = parser.accepts("disable-rack-aware", "Disable rack aware replica assignment");
        interBrokerThrottleOpt = parser.accepts("throttle", "The movement of partitions between brokers will be throttled to this value (bytes/sec). " +
                "This option can be included with --execute when a reassignment is started, and it can be altered by resubmitting the current reassignment " +
                "along with the --additional flag. The throttle rate should be at least 1 KB/s.")
            .withRequiredArg()
            .describedAs("throttle")
            .ofType(Long.class)
            .defaultsTo(-1L);
        replicaAlterLogDirsThrottleOpt = parser.accepts("replica-alter-log-dirs-throttle",
                "The movement of replicas between log directories on the same broker will be throttled to this value (bytes/sec). " +
                    "This option can be included with --execute when a reassignment is started, and it can be altered by resubmitting the current reassignment " +
                    "along with the --additional flag. The throttle rate should be at least 1 KB/s.")
            .withRequiredArg()
            .describedAs("replicaAlterLogDirsThrottle")
            .ofType(Long.class)
            .defaultsTo(-1L);
        timeoutOpt = parser.accepts("timeout", "The maximum time in ms to wait for log directory replica assignment to begin.")
            .withRequiredArg()
            .describedAs("timeout")
            .ofType(Long.class)
            .defaultsTo(10000L);
        additionalOpt = parser.accepts("additional", "Execute this reassignment in addition to any " +
            "other ongoing ones. This option can also be used to change the throttle of an ongoing reassignment.");
        preserveThrottlesOpt = parser.accepts("preserve-throttles", "Do not modify broker or topic throttles.");

        options = parser.parse(args);
    }

    public OptionSpecBuilder verifyOpt() {
        return verifyOpt;
    }

    public OptionSpecBuilder generateOpt() {
        return generateOpt;
    }

    public OptionSpecBuilder executeOpt() {
        return executeOpt;
    }

    public OptionSpecBuilder cancelOpt() {
        return cancelOpt;
    }

    public OptionSpecBuilder listOpt() {
        return listOpt;
    }

    public ArgumentAcceptingOptionSpec<String> bootstrapServerOpt() {
        return bootstrapServerOpt;
    }

    public ArgumentAcceptingOptionSpec<String> commandConfigOpt() {
        return commandConfigOpt;
    }

    public ArgumentAcceptingOptionSpec<String> reassignmentJsonFileOpt() {
        return reassignmentJsonFileOpt;
    }

    public ArgumentAcceptingOptionSpec<String> topicsToMoveJsonFileOpt() {
        return topicsToMoveJsonFileOpt;
    }

    public ArgumentAcceptingOptionSpec<String> brokerListOpt() {
        return brokerListOpt;
    }

    public OptionSpecBuilder disableRackAware() {
        return disableRackAware;
    }

    public ArgumentAcceptingOptionSpec<Long> interBrokerThrottleOpt() {
        return interBrokerThrottleOpt;
    }

    public ArgumentAcceptingOptionSpec<Long> replicaAlterLogDirsThrottleOpt() {
        return replicaAlterLogDirsThrottleOpt;
    }

    public ArgumentAcceptingOptionSpec<Long> timeoutOpt() {
        return timeoutOpt;
    }

    public OptionSpecBuilder additionalOpt() {
        return additionalOpt;
    }

    public OptionSpecBuilder preserveThrottlesOpt() {
        return preserveThrottlesOpt;
    }
}
