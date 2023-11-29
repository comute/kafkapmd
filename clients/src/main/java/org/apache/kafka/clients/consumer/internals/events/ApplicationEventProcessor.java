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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.CachedSupplier;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.MembershipManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * An {@link EventProcessor} that is created and executes in the {@link ConsumerNetworkThread network thread}
 * which processes {@link ApplicationEvent application events} generated by the application thread.
 */
public class ApplicationEventProcessor extends EventProcessor<ApplicationEvent> {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final RequestManagers requestManagers;
    private final BackgroundEventHandler backgroundEventHandler;

    public ApplicationEventProcessor(final LogContext logContext,
                                     final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                     final BackgroundEventHandler backgroundEventHandler,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata) {
        super(new LogContext("[Application event processor]" + (logContext.logPrefix() != null ? " " + logContext.logPrefix() : "")), applicationEventQueue);
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.backgroundEventHandler = backgroundEventHandler;
        this.requestManagers = requestManagers;
        this.metadata = metadata;
    }

    /**
     * Process the events—if any—that were produced by the application thread. It is possible that when processing
     * an event generates an error. In such cases, the processor will log an exception, but we do not want those
     * errors to be propagated to the caller.
     */
    public void process() {
        process((event, error) -> error.ifPresent(e -> log.warn("Error processing event {}", e.getMessage(), e)));
    }

    @Override
    public void process(ApplicationEvent event) {
        switch (event.type()) {
            case COMMIT:
                process((CommitApplicationEvent) event);
                return;

            case POLL:
                process((PollApplicationEvent) event);
                return;

            case FETCH_COMMITTED_OFFSET:
                process((OffsetFetchApplicationEvent) event);
                return;

            case METADATA_UPDATE:
                process((NewTopicsMetadataUpdateRequestEvent) event);
                return;

            case ASSIGNMENT_CHANGE:
                process((AssignmentChangeApplicationEvent) event);
                return;

            case TOPIC_METADATA:
                process((TopicMetadataApplicationEvent) event);
                return;

            case LIST_OFFSETS:
                process((ListOffsetsApplicationEvent) event);
                return;

            case RESET_POSITIONS:
                processResetPositionsEvent();
                return;

            case VALIDATE_POSITIONS:
                processValidatePositionsEvent();
                return;

            case SUBSCRIPTION_CHANGE:
                processSubscriptionChangeEvent();
                return;

            case UNSUBSCRIBE:
                processUnsubscribeEvent((UnsubscribeApplicationEvent) event);
                return;

            case CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED:
                process((ConsumerRebalanceListenerCallbackCompletedEvent) event);
                return;

            case WAIT_FOR_JOIN_GROUP:
                process((WaitForJoinGroupApplicationEvent) event);
                return;

            default:
                log.warn("Application event type " + event.type() + " was not expected");
        }
    }

    private void process(final PollApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.pollTimeMs());
    }

    private void process(final CommitApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            // Leaving this error handling here, but it is a bit strange as the commit API should enforce the group.id
            // upfront, so we should never get to this block.
            Exception exception = new KafkaException("Unable to commit offset. Most likely because the group.id wasn't set");
            event.future().completeExceptionally(exception);
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        event.chain(manager.addOffsetCommitRequest(event.offsets()));
    }

    private void process(final OffsetFetchApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed " +
                    "offset because the CommittedRequestManager is not available. Check if group.id was set correctly"));
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        event.chain(manager.addOffsetFetchRequest(event.partitions()));
    }

    private void process(final NewTopicsMetadataUpdateRequestEvent ignored) {
        metadata.requestUpdateForNewTopics();
    }

    private void process(final AssignmentChangeApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.currentTimeMs());
        manager.maybeAutoCommit(event.offsets());
    }

    private void process(final ListOffsetsApplicationEvent event) {
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> future =
                requestManagers.offsetsRequestManager.fetchOffsets(event.timestampsToSearch(),
                        event.requireTimestamps());
        event.chain(future);
    }

    /**
     * Process event that indicates that the subscription changed. This will make the
     * consumer join the group if it is not part of it yet, or send the updated subscription if
     * it is already a member.
     */
    private void processSubscriptionChangeEvent() {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing a subscribe event");
            backgroundEventHandler.add(new ErrorBackgroundEvent(error));
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
    private void processUnsubscribeEvent(UnsubscribeApplicationEvent event) {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an unsubscribe event");
            backgroundEventHandler.add(new ErrorBackgroundEvent(error));
            return;
        }

        MembershipManager membershipManager = requestManagers.heartbeatRequestManager.get().membershipManager();
        CompletableFuture<Void> result = membershipManager.leaveGroup();
        event.chain(result);
    }

    private void processResetPositionsEvent() {
        requestManagers.offsetsRequestManager.resetPositionsIfNeeded();
    }

    private void processValidatePositionsEvent() {
        requestManagers.offsetsRequestManager.validatePositionsIfNeeded();
    }

    private void process(final TopicMetadataApplicationEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestTopicMetadata(Optional.of(event.topic()));
        event.chain(future);
    }

    private void process(final ConsumerRebalanceListenerCallbackCompletedEvent event) {
        if (requestManagers.heartbeatRequestManager.isPresent()) {
            MembershipManager manager = requestManagers.heartbeatRequestManager.get().membershipManager();
            manager.consumerRebalanceListenerCallbackCompleted(event.methodName(), event.error());
        } else {
            log.warn(
                "An internal error occurred; the group membership manager was not present, so the notification of the {}.{} callback's execution could not be sent",
                ConsumerRebalanceListener.class.getSimpleName(),
                event.methodName()
            );
        }
    }

    private void process(final WaitForJoinGroupApplicationEvent event) {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            KafkaException error = new KafkaException("Group membership manager not present when waiting to join a group");
            backgroundEventHandler.add(new ErrorBackgroundEvent(error));
            return;
        }

        MembershipManager membershipManager = requestManagers.heartbeatRequestManager.get().membershipManager();
        membershipManager.notifyOnStable(event.future());
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<ApplicationEventProcessor> supplier(final LogContext logContext,
                                                               final ConsumerMetadata metadata,
                                                               final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                                               final BackgroundEventHandler backgroundEventHandler,
                                                               final Supplier<RequestManagers> requestManagersSupplier) {
        return new CachedSupplier<ApplicationEventProcessor>() {
            @Override
            protected ApplicationEventProcessor create() {
                RequestManagers requestManagers = requestManagersSupplier.get();
                return new ApplicationEventProcessor(
                        logContext,
                        applicationEventQueue,
                        backgroundEventHandler,
                        requestManagers,
                        metadata
                );
            }
        };
    }
}
