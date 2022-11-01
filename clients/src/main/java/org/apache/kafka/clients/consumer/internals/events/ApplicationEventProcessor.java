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
import org.apache.kafka.clients.consumer.internals.DefaultAsyncCoordinator;
import org.apache.kafka.clients.consumer.internals.NoopBackgroundEvent;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

public class ApplicationEventProcessor {
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final DefaultAsyncCoordinator coordinator;

    public ApplicationEventProcessor(
            final DefaultAsyncCoordinator coordinator,
            final BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.coordinator = coordinator;
        this.backgroundEventQueue = backgroundEventQueue;
    }
    public boolean process(final ApplicationEvent event) {
        Objects.requireNonNull(event);
        switch (event.type) {
            case NOOP:
                return process((NoopApplicationEvent) event);
            case COMMIT:
                return process((CommitApplicationEvent) event);
        }
        return false;
    }

    /**
     * Processes {@link NoopApplicationEvent} and equeue a
     * {@link NoopBackgroundEvent}. This is intentionally left here for
     * demonstration purpose.
     *
     * @param event a {@link NoopApplicationEvent}
     */
    private boolean process(final NoopApplicationEvent event) {
        return backgroundEventQueue.add(new NoopBackgroundEvent(event.message));
    }

    private boolean process(final CommitApplicationEvent event) {
        Map<TopicPartition, OffsetAndMetadata> offsets = event.offsets;
        coordinator.commitOffsets(offsets, event.callback, event.commitFuture);
        return true;
    }
}
