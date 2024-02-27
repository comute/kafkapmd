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

import org.apache.kafka.common.utils.Timer;

import java.util.concurrent.CompletableFuture;

/**
 * Background event with a result in the form of a future, that can be retrieved within a
 * timeout based on completion.
 *
 * @param <T>
 */
public abstract class CompletableBackgroundEvent<T> extends BackgroundEvent implements CompletableEvent<T> {

    private final CompletableFuture<T> future;
    private final long deadlineMs;

    protected CompletableBackgroundEvent(BackgroundEventType type, Timer timer) {
        super(type);
        this.future = new CompletableFuture<>();
        this.deadlineMs = timer.remainingMs() + timer.currentTimeMs();
    }

    @Override
    public CompletableFuture<T> future() {
        return future;
    }

    @Override
    public long deadlineMs() {
        return deadlineMs;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", future=" + future + ", deadlineMs=" + deadlineMs;
    }
}
