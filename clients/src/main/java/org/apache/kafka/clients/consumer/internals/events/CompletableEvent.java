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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * {@code CompletableEvent} is an interface that is used by both {@link CompletableApplicationEvent} and
 * {@link CompletableBackgroundEvent} for common processing and logic. A {@code CompletableEvent} is one that
 * allows the caller to get the {@link #future() future} related to the event and the event's
 * {@link #deadlineMs() expiration timestamp}.
 *
 * @param <T> Return type for the event when completed
 */
public interface CompletableEvent<T> {

    /**
     * Returns the {@link CompletableFuture future} associated with this event. Any event will have some related
     * logic that is executed on its behalf. The event can complete in one of the following ways:
     *
     * <ul>
     *     <li>
     *         Success: when the logic for the event completes successfully, the data generated by that event
     *         (if applicable) is passed to {@link CompletableFuture#complete(Object)}. In the case where the generic
     *         bound type is specified as {@link Void}, {@code null} is provided.</li>
     *     <li>
     *         Error: when the the event logic generates an error, the error is passed to
     *         {@link CompletableFuture#completeExceptionally(Throwable)}.
     *     </li>
     *     <li>
     *         Timeout: when the time spent executing the event logic exceeds the {@link #deadlineMs() deadline}, an
     *         instance of {@link TimeoutException} should be created and passed to
     *         {@link CompletableFuture#completeExceptionally(Throwable)}.
     *     </li>
     *     <li>
     *         Cancelled: when an event remains incomplete when the consumer closes, the future will be
     *         {@link CompletableFuture#cancel(boolean) cancelled}. Attempts to {@link Future#get() get the result}
     *         of the processing will throw a {@link CancellationException}.
     *     </li>
     * </ul>
     *
     * @return Future on which the caller may block or query for completion
     *
     * @see CompletableEventReaper
     */
    CompletableFuture<T> future();

    /**
     * This is the deadline that represents the absolute wall clock time by which any event-specific execution should
     * complete. This is not a timeout value. <em>After</em> this time has passed,
     * {@link CompletableFuture#completeExceptionally(Throwable)} will be invoked with an instance of
     * {@link TimeoutException}.
     *
     * @return Absolute time for event to be completed
     *
     * @see CompletableEventReaper
     */
    long deadlineMs();
}
