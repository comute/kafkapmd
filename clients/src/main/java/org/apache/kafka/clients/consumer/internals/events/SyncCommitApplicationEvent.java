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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Timer;

import java.util.Map;

import static org.apache.kafka.clients.consumer.internals.events.ApplicationEvent.Type.COMMIT_SYNC;

/**
 * Event to commit offsets waiting for a response and retrying on expected retriable errors until
 * the timer expires.
 */
public class SyncCommitApplicationEvent extends CommitApplicationEvent {

    public SyncCommitApplicationEvent(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                      final Timer timer) {
        super(COMMIT_SYNC, timer, offsets);
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", offsets=" + offsets();
    }
}
