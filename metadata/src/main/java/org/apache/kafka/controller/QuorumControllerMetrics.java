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
    private final static MetricName ACTIVE_CONTROLLER_COUNT = getMetricName(
        "KafkaController", "ActiveControllerCount");
    private final static MetricName EVENT_QUEUE_TIME_MS = getMetricName(
        "ControllerEventManager", "EventQueueTimeMs");
    private final static MetricName EVENT_QUEUE_PROCESSING_TIME_MS = getMetricName(
        "ControllerEventManager", "EventQueueProcessingTimeMs");
    private final static MetricName GLOBAL_TOPIC_COUNT = getMetricName(
        "KafkaController", "GlobalTopicCount");
    private final static MetricName GLOBAL_PARTITION_COUNT = getMetricName(
        "KafkaController", "GlobalPartitionCount");
    private final static MetricName OFFLINE_PARTITION_COUNT = getMetricName(
        "KafkaController", "OfflinePartitionsCount");
    private final static MetricName PREFERRED_REPLICA_IMBALANCE_COUNT = getMetricName(
        "KafkaController", "PreferredReplicaImbalanceCount");
    
    private volatile boolean active;
    private volatile int globalTopicCount;
    private volatile int globalPartitionCount;
    private volatile int offlinePartitionCount;
    private volatile int preferredReplicaImbalanceCount;
    private final Gauge<Integer> activeControllerCount;
    private final Gauge<Integer> globalPartitionCountGauge;
    private final Gauge<Integer> globalTopicCountGauge;
    private final Gauge<Integer> offlinePartitionCountGauge;
    private final Gauge<Integer> preferredReplicaImbalanceCountGauge;
    private final Histogram eventQueueTime;
    private final Histogram eventQueueProcessingTime;

    public QuorumControllerMetrics(MetricsRegistry registry) {
        this.active = false;
        this.globalTopicCount = 0;
        this.globalPartitionCount = 0;
        this.offlinePartitionCount = 0;
        this.preferredReplicaImbalanceCount = 0;
        this.activeControllerCount = registry.newGauge(ACTIVE_CONTROLLER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return active ? 1 : 0;
            }
        });
        this.eventQueueTime = registry.newHistogram(EVENT_QUEUE_TIME_MS, true);
        this.eventQueueProcessingTime = registry.newHistogram(EVENT_QUEUE_PROCESSING_TIME_MS, true);
        this.globalTopicCountGauge = registry.newGauge(GLOBAL_TOPIC_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return globalTopicCount;
            }
        });
        this.globalPartitionCountGauge = registry.newGauge(GLOBAL_PARTITION_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return globalPartitionCount;
            }
        });
        this.offlinePartitionCountGauge = registry.newGauge(OFFLINE_PARTITION_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return offlinePartitionCount;
            }
        });
        this.preferredReplicaImbalanceCountGauge = registry.newGauge(PREFERRED_REPLICA_IMBALANCE_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return preferredReplicaImbalanceCount;
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
        this.globalTopicCount = topicCount;
    }

    @Override
    public int globalTopicsCount() {
        return this.globalTopicCount;
    }

    @Override
    public void setGlobalPartitionCount(int partitionCount) {
        this.globalPartitionCount = partitionCount;
    }

    @Override
    public int globalPartitionCount() {
        return this.globalPartitionCount;
    }

    @Override
    public void setOfflinePartitionCount(int offlinePartitions) {
        this.offlinePartitionCount = offlinePartitions;
    }

    @Override
    public int offlinePartitionCount() {
        return this.offlinePartitionCount;
    }

    @Override
    public void setPreferredReplicaImbalanceCount(int replicaImbalances) {
        this.preferredReplicaImbalanceCount = replicaImbalances;
    }

    @Override
    public int preferredReplicaImbalanceCount() {
        return this.preferredReplicaImbalanceCount;
    }

    private static MetricName getMetricName(String type, String name) {
        final String group = "kafka.controller";
        final StringBuilder mbeanNameBuilder = new StringBuilder();
        mbeanNameBuilder.append(group).append(":type=").append(type).append(",name=").append(name);
        return new MetricName(group, type, name, null, mbeanNameBuilder.toString());
    }
}
