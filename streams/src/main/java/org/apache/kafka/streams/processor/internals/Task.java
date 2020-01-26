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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public interface Task {

    long LATEST_OFFSET = -2L;

    enum State {
        CREATED(1, 4) {         // 0
            @Override
            public boolean hasBeenRunning() {
                return false;
            }
        },
        RESTORING(2, 3, 4) {    // 1
            @Override
            public boolean hasBeenRunning() {
                return false;
            }
        },
        RUNNING(3, 4) {         // 2
            @Override
            public boolean hasBeenRunning() {
                return true;
            }
        },
        SUSPENDED(1, 4) {       // 3
            @Override
            public boolean hasBeenRunning() {
                return true;
            }
        },
        CLOSING(4, 5) {         // 4, we allow CLOSING to transit to itself to make close idempotent
            @Override
            public boolean hasBeenRunning() {
                return false;
            }
        },
        CLOSED {                               // 5
            @Override
            public boolean hasBeenRunning() {
                return true;
            }
        };

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isValidTransition(final State newState) {
            return validTransitions.contains(newState.ordinal());
        }

        public abstract boolean hasBeenRunning();
    }

    enum TaskType {
        ACTIVE("ACTIVE"),

        STANDBY("STANDBY"),

        GLOBAL("GLOBAL");

        public final String name;

        TaskType(final String name) {
            this.name = name;
        }
    }

    TaskId id();

    State state();

    boolean isActive();

    void initializeIfNeeded();

    void completeRestoration();

    void addRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> records);

    boolean commitNeeded();

    void commit();

    void suspend();

    void resume();

    /**
     * Close a task that we still own. Commit all progress and close the task gracefully.
     * Throws an exception if this couldn't be done.
     */
    void closeClean();

    /**
     * Close a task that we may not own. Discard any uncommitted progress and close the task.
     * Never throws an exception, but just makes all attempts to release resources while closing.
     */
    void closeDirty();

    StateStore getStore(final String name);

    Set<TopicPartition> inputPartitions();

    /**
     * @return any changelog partitions associated with this task
     */
    Collection<TopicPartition> changelogPartitions();

    /**
     * @return the offsets of all the changelog partitions associated with this task,
     *         indicating the current positions of the logged state stores of the task.
     */
    Map<TopicPartition, Long> changelogOffsets();

    default Map<TopicPartition, Long> purgableOffsets() {
        return Collections.emptyMap();
    }

    default boolean process(final long wallClockTime) {
        return false;
    }

    default boolean commitRequested() {
        return false;
    }

    default boolean maybePunctuateStreamTime() {
        return false;
    }

    default boolean maybePunctuateSystemTime() {
        return false;
    }


}
