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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

@InterfaceStability.Evolving
public class DescribeProducersResult {

    private final Map<TopicPartition, KafkaFutureImpl<PartitionProducerState>> futures;

    DescribeProducersResult(Map<TopicPartition, KafkaFutureImpl<PartitionProducerState>> futures) {
        this.futures = futures;
    }

    public KafkaFuture<PartitionProducerState> partitionResult(final TopicPartition partition) {
        KafkaFuture<PartitionProducerState> future = futures.get(partition);
        if (future == null) {
            throw new IllegalArgumentException("Topic partition " + partition +
                " was not included in the request");
        }
        return future;
    }

    public KafkaFuture<Map<TopicPartition, PartitionProducerState>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
            .thenApply(nil -> {
                Map<TopicPartition, PartitionProducerState> results = new HashMap<>(futures.size());
                for (Map.Entry<TopicPartition, KafkaFutureImpl<PartitionProducerState>> entry : futures.entrySet()) {
                    try {
                        results.put(entry.getKey(), entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, because allOf ensured that all the futures completed successfully.
                        throw new KafkaException(e);
                    }
                }
                return results;
            });
    }

    public static class PartitionProducerState {
        private final List<ProducerState> activeProducers;

        public PartitionProducerState(List<ProducerState> activeProducers) {
            this.activeProducers = activeProducers;
        }

        public List<ProducerState> activeProducers() {
            return activeProducers;
        }
    }

    public static class ProducerState {
        private final long producerId;
        private final int producerEpoch;
        private final int lastSequence;
        private final long lastTimestamp;
        private final OptionalInt coordinatorEpoch;
        private final OptionalLong currentTransactionStartOffset;

        public ProducerState(
            long producerId,
            int producerEpoch,
            int lastSequence,
            long lastTimestamp,
            OptionalInt coordinatorEpoch,
            OptionalLong currentTransactionStartOffset
        ) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.lastSequence = lastSequence;
            this.lastTimestamp = lastTimestamp;
            this.coordinatorEpoch = coordinatorEpoch;
            this.currentTransactionStartOffset = currentTransactionStartOffset;
        }

        public long producerId() {
            return producerId;
        }

        public int producerEpoch() {
            return producerEpoch;
        }

        public int lastSequence() {
            return lastSequence;
        }

        public long lastTimestamp() {
            return lastTimestamp;
        }

        public OptionalLong currentTransactionStartOffset() {
            return currentTransactionStartOffset;
        }

        public OptionalInt coordinatorEpoch() {
            return coordinatorEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProducerState that = (ProducerState) o;
            return producerId == that.producerId &&
                producerEpoch == that.producerEpoch &&
                lastSequence == that.lastSequence &&
                lastTimestamp == that.lastTimestamp &&
                Objects.equals(coordinatorEpoch, that.coordinatorEpoch) &&
                Objects.equals(currentTransactionStartOffset, that.currentTransactionStartOffset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(producerId, producerEpoch, lastSequence, lastTimestamp,
                coordinatorEpoch, currentTransactionStartOffset);
        }

        @Override
        public String toString() {
            return "ProducerState(" +
                "producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", lastSequence=" + lastSequence +
                ", lastTimestamp=" + lastTimestamp +
                ", coordinatorEpoch=" + coordinatorEpoch +
                ", currentTransactionStartOffset=" + currentTransactionStartOffset +
                ')';
        }
    }

}
