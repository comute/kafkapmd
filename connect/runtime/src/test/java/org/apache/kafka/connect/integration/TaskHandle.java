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
package org.apache.kafka.connect.integration;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A handle to an executing task in a worker. Use this class to record progress, for example: number of records seen
 * by the task using so far, or waiting for partitions to be assigned to the task.
 */
public class TaskHandle {

    private static final Logger log = LoggerFactory.getLogger(TaskHandle.class);

    private final String taskId;
    private final CountDownLatch expectedPartitionsLatch;

    private CountDownLatch recordsRemainingLatch;
    private int expectedRecords = -1;

    public TaskHandle(String taskId) {
        this.taskId = taskId;
        this.expectedPartitionsLatch = new CountDownLatch(1);
    }

    /**
     * Decrement the number of records seen by this task.
     */
    public void record() {
        if (recordsRemainingLatch != null) {
            recordsRemainingLatch.countDown();
        }
    }

    /**
     * Set the number of expected records for this task.
     *
     * @param expectedRecords number of records
     */
    public void expectedRecords(int expectedRecords) {
        this.expectedRecords = expectedRecords;
        this.recordsRemainingLatch = new CountDownLatch(expectedRecords);
    }

    /**
     * Set the number of partitions assigned to this task.
     *
     * @param numPartitions number of partitions
     */
    public void partitionsAssigned(int numPartitions) {
        for (int i = 0; i < numPartitions; i++) {
            expectedPartitionsLatch.countDown();
        }
    }

    /**
     * Wait for this task to be assigned partitions.
     *
     * @param consumeMaxDurationMs max duration to wait for partition assignment.
     * @throws InterruptedException if another threads interrupts this one while waiting for partitions to be assigned
     */
    public void awaitPartitionAssignment(int consumeMaxDurationMs) throws InterruptedException {
        if (!expectedPartitionsLatch.await(consumeMaxDurationMs, TimeUnit.MILLISECONDS)) {
            String msg = String.format("No partitions were assigned to task %s in %d millis.",
                    taskId,
                    consumeMaxDurationMs);
            throw new DataException(msg);
        }
        log.debug("Task {} saw {} records, expected {} records", taskId, expectedRecords - recordsRemainingLatch.getCount(), expectedRecords);
    }

    /**
     * Wait for this task to receive the expected number of records.
     *
     * @param consumeMaxDurationMs max duration to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(int consumeMaxDurationMs) throws InterruptedException {
        if (recordsRemainingLatch == null) {
            throw new IllegalStateException("Illegal state encountered. expectedRecords() was not set for this task?");
        }
        if (!recordsRemainingLatch.await(consumeMaxDurationMs, TimeUnit.MILLISECONDS)) {
            String msg = String.format("Insufficient records seen by task %s in %d millis. Records expected=%d, actual=%d",
                    taskId,
                    consumeMaxDurationMs,
                    expectedRecords,
                    expectedRecords - recordsRemainingLatch.getCount());
            throw new DataException(msg);
        }
        log.debug("Task {} saw {} records, expected {} records", taskId, expectedRecords - recordsRemainingLatch.getCount(), expectedRecords);
    }

    @Override
    public String toString() {
        return "Handle{" +
                "taskId='" + taskId + '\'' +
                '}';
    }
}
