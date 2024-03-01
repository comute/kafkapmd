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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.CachedSupplier;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.MembershipManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An {@link EventProcessor} that is created and executes in the {@link ConsumerNetworkThread network thread}
 * which processes {@link ApplicationEvent application events} generated by the application thread.
 */
public class ApplicationEventProcessor extends EventProcessor<ApplicationEvent> {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final RequestManagers requestManagers;
    private final List<CompletableApplicationEvent<?>> completableEvents;

    public ApplicationEventProcessor(final LogContext logContext,
                                     final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata) {
        super(logContext, applicationEventQueue);
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.requestManagers = requestManagers;
        this.metadata = metadata;
        this.completableEvents = new LinkedList<>();
    }

    /**
     * Process the events—if any—that were produced by the application thread. It is possible that when processing
     * an event generates an error. In such cases, the processor will log an exception, but we do not want those
     * errors to be propagated to the caller.
     */
    public void process() {
        process((event, error) -> {
            if (event instanceof CompletableEvent)
                completableEvents.add((CompletableApplicationEvent<?>) event);

            error.ifPresent(e -> log.warn("Error processing event {}", e.getMessage(), e));
        });
    }

    @SuppressWarnings({"CyclomaticComplexity"})
    @Override
    public void process(ApplicationEvent event) {
        switch (event.type()) {
            case COMMIT_ASYNC:
                process((AsyncCommitEvent) event);
                return;

            case COMMIT_SYNC:
                process((SyncCommitEvent) event);
                return;

            case POLL:
                process((PollEvent) event);
                return;

            case FETCH_COMMITTED_OFFSETS:
                process((FetchCommittedOffsetsEvent) event);
                return;

            case NEW_TOPICS_METADATA_UPDATE:
                process((NewTopicsMetadataUpdateRequestEvent) event);
                return;

            case ASSIGNMENT_CHANGE:
                process((AssignmentChangeEvent) event);
                return;

            case TOPIC_METADATA:
                process((TopicMetadataEvent) event);
                return;

            case ALL_TOPICS_METADATA:
                process((AllTopicsMetadataEvent) event);
                return;

            case LIST_OFFSETS:
                process((ListOffsetsEvent) event);
                return;

            case RESET_POSITIONS:
                process((ResetPositionsEvent) event);
                return;

            case VALIDATE_POSITIONS:
                process((ValidatePositionsEvent) event);
                return;

            case SUBSCRIPTION_CHANGE:
                process((SubscriptionChangeEvent) event);
                return;

            case UNSUBSCRIBE:
                process((UnsubscribeEvent) event);
                return;

            case CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED:
                process((ConsumerRebalanceListenerCallbackCompletedEvent) event);
                return;

            case COMMIT_ON_CLOSE:
                process((CommitOnCloseEvent) event);
                return;

            case LEAVE_ON_CLOSE:
                process((LeaveOnCloseEvent) event);
                return;

            default:
                log.warn("Application event type " + event.type() + " was not expected");
        }
    }

    /**
     * We "complete" any of the {@link CompletableApplicationEvent}s that are either expired or done.
     *
     * <p/>
     *
     * <em>Note</em>: this cleanup step should be called <em>after</b> the user has processed the  {@link #process(ProcessHandler)} is executed.
     * The reason for this is to emulate the behavior of the legacy consumer's handling of timeouts. The legacy
     * consumer would
     * difference between the so that we can perform this cleanup after  any events t
     *
     * @param currentTimeMs Current time
     */
    public void completeExpiredEvents(long currentTimeMs) {
        log.trace("Removing expired events");

        Consumer<CompletableApplicationEvent<?>> completeEvent = e -> {
            long pastDueMs = currentTimeMs - e.deadlineMs();
            log.debug("Completing event {} exceptionally since it expired {} ms ago", e, pastDueMs);
            CompletableFuture<?> f = e.future();
            f.completeExceptionally(new TimeoutException(String.format("%s could not be completed within its timeout", e.getClass().getSimpleName())));
        };

        // First, complete (exceptionally) any events that have passed their deadline.
        completableEvents
                .stream()
                .filter(e -> !e.future().isDone() && currentTimeMs > e.deadlineMs())
                .forEach(completeEvent);

        // Second, remove any events that are already done, just to make sure we don't hold references.
        completableEvents.removeIf(e -> e.future().isDone());
        log.trace("Finished removal of expired events");
    }

    private void process(final PollEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        requestManagers.commitRequestManager.ifPresent(m -> m.updateAutoCommitTimer(event.pollTimeMs()));
        requestManagers.heartbeatRequestManager.ifPresent(hrm -> hrm.resetPollTimer(event.pollTimeMs()));
    }

    private void process(final AsyncCommitEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Void> future = manager.commitAsync(event.offsets());
        future.whenComplete(complete(event.future()));
    }

    private void process(final SyncCommitEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Void> future = manager.commitSync(event.offsets(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final FetchCommittedOffsetsEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed " +
                    "offset because the CommittedRequestManager is not available. Check if group.id was set correctly"));
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = manager.fetchOffsets(
            event.partitions(),
            event.deadlineMs()
        );
        future.whenComplete(complete(event.future()));
    }

    private void process(final NewTopicsMetadataUpdateRequestEvent ignored) {
        metadata.requestUpdateForNewTopics();
    }

    /**
     * Commit all consumed if auto-commit is enabled. Note this will trigger an async commit,
     * that will not be retried if the commit request fails.
     */
    private void process(final AssignmentChangeEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.currentTimeMs());
        manager.maybeAutoCommitAsync();
    }

    private void process(final ListOffsetsEvent event) {
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> future = requestManagers.offsetsRequestManager.fetchOffsets(
            event.timestampsToSearch(),
            event.requireTimestamps()
        );
        future.whenComplete(complete(event.future()));
    }

    /**
     * Process event that indicates that the subscription changed. This will make the
     * consumer join the group if it is not part of it yet, or send the updated subscription if
     * it is already a member.
     */
    private void process(final SubscriptionChangeEvent ignored) {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            return;
        }
        MembershipManager membershipManager = requestManagers.heartbeatRequestManager.get().membershipManager();
        membershipManager.onSubscriptionUpdated();
    }

    /**
     * Process event indicating that the consumer unsubscribed from all topics. This will make
     * the consumer release its assignment and send a request to leave the group.
     *
     * @param event Unsubscribe event containing a future that will complete when the callback
     *              execution for releasing the assignment completes, and the request to leave
     *              the group is sent out.
     */
    private void process(final UnsubscribeEvent event) {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an unsubscribe event");
            event.future().completeExceptionally(error);
            return;
        }
        MembershipManager membershipManager = requestManagers.heartbeatRequestManager.get().membershipManager();
        CompletableFuture<Void> future = membershipManager.leaveGroup();
        future.whenComplete(complete(event.future()));
    }

    private void process(final ResetPositionsEvent event) {
        CompletableFuture<Void> future = requestManagers.offsetsRequestManager.resetPositionsIfNeeded();
        future.whenComplete(complete(event.future()));
    }

    private void process(final ValidatePositionsEvent event) {
        CompletableFuture<Void> future = requestManagers.offsetsRequestManager.validatePositionsIfNeeded();
        future.whenComplete(complete(event.future()));
    }

    private void process(final TopicMetadataEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestTopicMetadata(event.topic(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final AllTopicsMetadataEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestAllTopicsMetadata(event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final ConsumerRebalanceListenerCallbackCompletedEvent event) {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            log.warn(
                "An internal error occurred; the group membership manager was not present, so the notification of the {} callback execution could not be sent",
                event.methodName()
            );
            return;
        }
        MembershipManager manager = requestManagers.heartbeatRequestManager.get().membershipManager();
        manager.consumerRebalanceListenerCallbackCompleted(event);
    }

    private void process(final CommitOnCloseEvent event) {
        if (!requestManagers.commitRequestManager.isPresent())
            return;
        log.debug("Signal CommitRequestManager closing");
        requestManagers.commitRequestManager.get().signalClose();
    }

    private void process(final LeaveOnCloseEvent event) {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            event.future().complete(null);
            return;
        }
        MembershipManager membershipManager =
            Objects.requireNonNull(requestManagers.heartbeatRequestManager.get().membershipManager(), "Expecting " +
                "membership manager to be non-null");
        log.debug("Leaving group before closing");
        CompletableFuture<Void> future = membershipManager.leaveGroup();
        // The future will be completed on heartbeat sent
        future.whenComplete(complete(event.future()));
    }

    private <T> BiConsumer<? super T, ? super Throwable> complete(final CompletableFuture<T> f) {
        return (value, exception) -> {
            if (exception != null)
                f.completeExceptionally(exception);
            else
                f.complete(value);
        };
    }

    /**
     * Check each of the {@link CompletableApplicationEvent completable events}, and for any that are
     * incomplete, {@link CompletableFuture#completeExceptionally(Throwable) complete it exceptionally}.
     *
     * <p/>
     *
     * <em>Note</em>: because this is called in the context of {@link AsyncKafkaConsumer#close() closing consumer},
     * don't take the deadline into consideration, just close it regardless.
     */
    @Override
    protected void cancelIncompleteEvents() {
        super.cancelIncompleteEvents();

        completableEvents
                .stream()
                .filter(e -> !e.future().isDone())
                .forEach(INCOMPLETE_EVENT_CANCELLER);
        completableEvents.clear();
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<ApplicationEventProcessor> supplier(final LogContext logContext,
                                                               final ConsumerMetadata metadata,
                                                               final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                                               final Supplier<RequestManagers> requestManagersSupplier) {
        return new CachedSupplier<ApplicationEventProcessor>() {
            @Override
            protected ApplicationEventProcessor create() {
                RequestManagers requestManagers = requestManagersSupplier.get();
                return new ApplicationEventProcessor(
                        logContext,
                        applicationEventQueue,
                        requestManagers,
                        metadata
                );
            }
        };
    }
}
