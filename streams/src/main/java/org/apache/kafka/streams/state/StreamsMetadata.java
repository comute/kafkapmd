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
package org.apache.kafka.streams.state;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Collections;
import java.util.Set;

/**
 * Represents the state of an instance (process) in a {@link KafkaStreams} application.
 * It contains the user supplied {@link HostInfo} that can be used by developers to build
 * APIs and services to connect to other instances, the Set of state stores available on
 * the instance and the Set of {@link TopicPartition}s available on the instance.
 * NOTE: This is a point in time view. It may change when rebalances happen.
 */
public class StreamsMetadata {
    /**
     * Sentinel to indicate that the StreamsMetadata is currently unavailable. This can occur during rebalance
     * operations.
     */
    public final static StreamsMetadata NOT_AVAILABLE = new StreamsMetadata(new HostInfo("unavailable", -1),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet());

    private final HostInfo hostInfo;
    /**
     * State stores owned by the instance as an active replica
     */
    private final Set<String> stateStoreNames;
    /**
     * Topic partitions consumed by the instance as an active replica
     */
    private final Set<TopicPartition> topicPartitions;
    /**
     * State stores owned by the instance as a standby replica
     */
    private final Set<String> standbyStateStoreNames;
    /**
     * (Source) Topic partitions for which the instance acts as standby.
     */
    private final Set<TopicPartition> standbyTopicPartitions;


    public StreamsMetadata(final HostInfo hostInfo,
                           final Set<String> stateStoreNames,
                           final Set<TopicPartition> topicPartitions,
                           final Set<TopicPartition> standbyTopicPartitions,
                           final Set<String> standbyStateStoreNames) {

        this.hostInfo = hostInfo;
        this.stateStoreNames = stateStoreNames;
        this.topicPartitions = topicPartitions;
        this.standbyTopicPartitions = standbyTopicPartitions;
        this.standbyStateStoreNames = standbyStateStoreNames;
    }

    public Set<TopicPartition> standbyTopicPartitions() {
        return standbyTopicPartitions;
    }

    public Set<String> standbyStateStoreNames() {
        return standbyStateStoreNames;
    }

    public HostInfo hostInfo() {
        return hostInfo;
    }

    public Set<String> stateStoreNames() {
        return stateStoreNames;
    }

    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public String host() {
        return hostInfo.host();
    }

    @SuppressWarnings("unused")
    public int port() {
        return hostInfo.port();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsMetadata that = (StreamsMetadata) o;
        if (!hostInfo.equals(that.hostInfo)) {
            return false;
        }
        if (!stateStoreNames.equals(that.stateStoreNames)) {
            return false;
        }
        if (!topicPartitions.equals(that.topicPartitions)) {
            return false;
        }
        if (!standbyStateStoreNames.equals(that.standbyStateStoreNames)) {
            return false;
        }

        return standbyTopicPartitions.equals(that.standbyTopicPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostInfo, stateStoreNames, topicPartitions, standbyStateStoreNames, standbyTopicPartitions);
    }

    @Override
    public String toString() {
        return "StreamsMetadata {" +
                "hostInfo=" + hostInfo +
                ", stateStoreNames=" + stateStoreNames +
                ", topicPartitions=" + topicPartitions +
                ", standbyStateStoreNames=" + standbyStateStoreNames +
                ", standbyTopicPartitions=" + standbyTopicPartitions +
                '}';
    }
}
