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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.common.Topology;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * The specification for a workload that sends messages to a broker and then
 * reads them back.
 */
public class RoundTripWorkloadSpec extends TaskSpec {
    private final String clientNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final NavigableMap<Integer, List<Integer>> partitionAssignments;
    private final PayloadGenerator valueGenerator;
    private final int maxMessages;
    private final Map<String, String> commonClientConf;
    private final Map<String, String> producerConf;
    private final Map<String, String> consumerConf;
    private final Map<String, String> adminClientConf;

    @JsonCreator
    public RoundTripWorkloadSpec(@JsonProperty("startMs") long startMs,
             @JsonProperty("durationMs") long durationMs,
             @JsonProperty("clientNode") String clientNode,
             @JsonProperty("bootstrapServers") String bootstrapServers,
             @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
             @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
             @JsonProperty("consumerConf") Map<String, String> consumerConf,
             @JsonProperty("producerConf") Map<String, String> producerConf,
             @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
             @JsonProperty("partitionAssignments") NavigableMap<Integer, List<Integer>> partitionAssignments,
             @JsonProperty("valueGenerator") PayloadGenerator valueGenerator,
             @JsonProperty("maxMessages") int maxMessages) {
        super(startMs, durationMs);
        this.clientNode = clientNode == null ? "" : clientNode;
        this.bootstrapServers = bootstrapServers == null ? "" : bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.partitionAssignments = partitionAssignments == null ?
            new TreeMap<Integer, List<Integer>>() : partitionAssignments;
        this.valueGenerator = valueGenerator == null ?
            new UniformRandomPayloadGenerator(32, 123, 10) : valueGenerator;
        this.maxMessages = maxMessages;
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.producerConf = configOrEmptyMap(producerConf);
        this.consumerConf = configOrEmptyMap(consumerConf);
    }

    @JsonProperty
    public String clientNode() {
        return clientNode;
    }

    @JsonProperty
    public String bootstrapServers() {
        return bootstrapServers;
    }

    @JsonProperty
    public int targetMessagesPerSec() {
        return targetMessagesPerSec;
    }

    @JsonProperty
    public NavigableMap<Integer, List<Integer>> partitionAssignments() {
        return partitionAssignments;
    }

    @JsonProperty
    public PayloadGenerator valueGenerator() {
        return valueGenerator;
    }

    @JsonProperty
    public int maxMessages() {
        return maxMessages;
    }

    @JsonProperty
    public Map<String, String> commonClientConf() {
        return commonClientConf;
    }

    @JsonProperty
    public Map<String, String> adminClientConf() {
        return adminClientConf;
    }

    @JsonProperty
    public Map<String, String> producerConf() {
        return producerConf;
    }

    @JsonProperty
    public Map<String, String> consumerConf() {
        return consumerConf;
    }

    @Override
    public TaskController newController(String id) {
        return new TaskController() {
            @Override
            public Set<String> targetNodes(Topology topology) {
                return Collections.singleton(clientNode);
            }
        };
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new RoundTripWorker(id, this);
    }
}
