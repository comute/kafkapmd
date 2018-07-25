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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A PartitionGroup is composed from a set of partitions. It also maintains the timestamp of this
 * group, hence the associated task as the min timestamp across all partitions in the group.
 */
public class PartitionGroup {

    private final Map<TopicPartition, RecordQueue> partitionQueues;
    private final PriorityQueue<RecordQueue> nonEmptyQueuesByTime;
    private final int numQueues;

    private long streamTime;
    private int totalBuffered;
    private boolean allBuffered;


    public static class RecordInfo {
        RecordQueue queue;

        public ProcessorNode node() {
            return queue.source();
        }

        public TopicPartition partition() {
            return queue.partition();
        }

        RecordQueue queue() {
            return queue;
        }
    }

    PartitionGroup(final Map<TopicPartition, RecordQueue> partitionQueues) {
        nonEmptyQueuesByTime = new PriorityQueue<>(partitionQueues.size(), Comparator.comparingLong(RecordQueue::timestamp));
        this.partitionQueues = partitionQueues;
        totalBuffered = 0;
        allBuffered = false;
        streamTime = RecordQueue.NOT_KNOWN;
        numQueues = this.partitionQueues.keySet().size();
    }

    /**
     * Get the next record and queue
     *
     * @return StampedRecord
     */
    StampedRecord nextRecord(final RecordInfo info) {
        StampedRecord record = null;

        final RecordQueue queue = nonEmptyQueuesByTime.poll();
        info.queue = queue;

        if (queue != null) {
            // get the first record from this queue.
            record = queue.poll();

            if (record != null) {
                --totalBuffered;

                if (!queue.isEmpty()) {
                    nonEmptyQueuesByTime.offer(queue);
                } else {
                    // if a certain queue has been drained, reset the flag
                    allBuffered = false;
                }

                // always update the stream time to the record's timestamp yet to be processed if it is larger
                streamTime = record.timestamp > streamTime ? record.timestamp : streamTime;
            }
        }

        return record;
    }

    /**
     * Adds raw records to this partition group
     *
     * @param partition the partition
     * @param rawRecords  the raw records
     * @return the queue size for the partition
     */
    int addRawRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        final RecordQueue recordQueue = partitionQueues.get(partition);

        final int oldSize = recordQueue.size();
        final int newSize = recordQueue.addRawRecords(rawRecords);

        // add this record queue to be considered for processing in the future if it was empty before
        if (oldSize == 0 && newSize > 0) {
            nonEmptyQueuesByTime.offer(recordQueue);

            // if all partitions now are non-empty, set the flag and compute the stream time
            if (nonEmptyQueuesByTime.size() == numQueues) {
                allBuffered = true;

                // since we may enforce processing even if some queue is empty, it is possible that after some
                // raw data has been added to that queue the new partition's timestamp is even smaller than the current
                // stream time, in this case we should not update.
                final long newTimestamp = nonEmptyQueuesByTime.peek().timestamp();
                streamTime = newTimestamp > streamTime ? newTimestamp : streamTime;
            }
        }

        totalBuffered += newSize - oldSize;

        return newSize;
    }

    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(partitionQueues.keySet());
    }

    /**
     * Return the timestamp of this partition group as the smallest
     * partition timestamp among all its partitions
     */
    public long timestamp() {
        return streamTime;
    }

    /**
     * @throws IllegalStateException if the record's partition does not belong to this partition group
     */
    int numBuffered(final TopicPartition partition) {
        final RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null) {
            throw new IllegalStateException(String.format("Record's partition %s does not belong to this partition-group.", partition));
        }

        return recordQueue.size();
    }

    int numBuffered() {
        return totalBuffered;
    }

    boolean allPartitionsBuffered() {
        return allBuffered;
    }

    public void close() {
        partitionQueues.clear();
    }

    public void clear() {
        nonEmptyQueuesByTime.clear();
        for (final RecordQueue queue : partitionQueues.values()) {
            queue.clear();
        }
    }
}
