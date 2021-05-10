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

package org.apache.kafka.controller;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;


public final class QuorumControllerMetrics implements ControllerMetrics {
    private final static MetricName ACTIVE_CONTROLLER_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "ActiveControllerCount", null);
    private final static MetricName EVENT_QUEUE_TIME_MS = new MetricName(
        "kafka.controller", "ControllerEventManager", "EventQueueTimeMs", null);
    private final static MetricName EVENT_QUEUE_PROCESSING_TIME_MS = new MetricName(
        "kafka.controller", "ControllerEventManager", "EventQueueProcessingTimeMs", null);
    private final static MetricName GLOBAL_TOPIC_COUNT = new MetricName(
        "kafka.controller", "ReplicationControlManager", "GlobalTopicCount", null);
    private final static MetricName GLOBAL_PARTITION_COUNT = new MetricName(
        "kafka.controller", "ReplicationControlManager", "GlobalPartitionCount", null);
    private final static MetricName OFFLINE_PARTITION_COUNT = new MetricName(
        "kafka.controller", "ReplicationControlManager", "OfflinePartitionCount", null);
    private final static MetricName PREFERRED_REPLICA_IMBALANCE_COUNT = new MetricName(
        "kafka.controller", "ReplicationControlManager", "PreferredReplicaImbalanceCount", null);
    

    private volatile boolean active;
    private volatile int topics;
    private volatile int partitions;
    private volatile int offlinePartitions;
    private volatile int preferredReplicaImbalances;
    private final Gauge<Integer> activeControllerCount;
    private final Gauge<Integer> globalPartitionCount;
    private final Gauge<Integer> globalTopicCount;
    private final Gauge<Integer> offlinePartitionCount;
    private final Gauge<Integer> preferredReplicaImbalanceCount;
    private final Histogram eventQueueTime;
    private final Histogram eventQueueProcessingTime;

    public QuorumControllerMetrics(MetricsRegistry registry) {
        this.active = false;
        this.topics = 0;
        this.partitions = 0;
        this.offlinePartitions = 0;
        this.preferredReplicaImbalances = 0;
        this.activeControllerCount = registry.newGauge(ACTIVE_CONTROLLER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return active ? 1 : 0;
            }
        });
        this.eventQueueTime = registry.newHistogram(EVENT_QUEUE_TIME_MS, true);
        this.eventQueueProcessingTime = registry.newHistogram(EVENT_QUEUE_PROCESSING_TIME_MS, true);
        this.globalTopicCount = registry.newGauge(GLOBAL_TOPIC_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return topics;
            }
        });
        this.globalPartitionCount = registry.newGauge(GLOBAL_PARTITION_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return partitions;
            }
        });
        this.offlinePartitionCount = registry.newGauge(OFFLINE_PARTITION_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return offlinePartitions;
            }
        });
        this.preferredReplicaImbalanceCount = registry.newGauge(PREFERRED_REPLICA_IMBALANCE_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return preferredReplicaImbalances;
            }
        });
    }

    @Override
    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public boolean active() {
        return this.active;
    }

    @Override
    public void updateEventQueueTime(long durationMs) {
        eventQueueTime.update(durationMs);
    }

    @Override
    public void updateEventQueueProcessingTime(long durationMs) {
        eventQueueTime.update(durationMs);
    }

    @Override
    public void setGlobalTopicsCount(int topicCount) {
        this.topics = topicCount;
    }

    @Override
    public int globalTopicsCount() {
        return this.topics;
    }

    @Override
    public void setGlobalPartitionCount(int partitionCount) {
        this.partitions = partitionCount;
    }

    @Override
    public int globalPartitionCount() {
        return this.partitions;
    }

    @Override
    public void setOfflinePartitionCount(int offlinePartitions) {
        this.offlinePartitions = offlinePartitions;
    }

    @Override
    public int offlinePartitionCount() {
        return this.offlinePartitions;
    }

    @Override
    public void setPreferredReplicaImbalanceCount(int replicaImbalances) {
        this.preferredReplicaImbalances = replicaImbalances;
    }

    @Override
    public int preferredReplicaImbalanceCount() {
        return this.preferredReplicaImbalances;
    }
}
