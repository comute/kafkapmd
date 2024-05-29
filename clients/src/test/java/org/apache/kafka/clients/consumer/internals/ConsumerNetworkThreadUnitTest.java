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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

public class ConsumerNetworkThreadUnitTest {

    private final Time time;
    private final ConsumerMetadata metadata;
    private final ConsumerConfig config;
    private final SubscriptionState subscriptions;
    private final BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private final ApplicationEventProcessor applicationEventProcessor;
    private final OffsetsRequestManager offsetsRequestManager;
    private final CommitRequestManager commitRequestManager;
    private final HeartbeatRequestManager heartbeatRequestManager;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final ConsumerNetworkThread consumerNetworkThread;
    private final MockClient client;
    private final NetworkClientDelegate networkClientDelegate;
    private final RequestManagers requestManagers;
    private final CompletableEventReaper applicationEventReaper;

    ConsumerNetworkThreadUnitTest() {
        LogContext logContext = new LogContext();
        this.time = new MockTime();
        this.client = new MockClient(time);
        this.networkClientDelegate = mock(NetworkClientDelegate.class);
        this.requestManagers = mock(RequestManagers.class);
        this.offsetsRequestManager = mock(OffsetsRequestManager.class);
        this.commitRequestManager = mock(CommitRequestManager.class);
        this.heartbeatRequestManager = mock(HeartbeatRequestManager.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.config = mock(ConsumerConfig.class);
        this.subscriptions = mock(SubscriptionState.class);
        this.metadata = mock(ConsumerMetadata.class);
        this.applicationEventProcessor = mock(ApplicationEventProcessor.class);
        this.applicationEventReaper = mock(CompletableEventReaper.class);

        this.consumerNetworkThread = new ConsumerNetworkThread(
                logContext,
                time,
                applicationEventsQueue,
                applicationEventReaper,
                () -> applicationEventProcessor,
                () -> networkClientDelegate,
                () -> requestManagers
        );
    }

    @BeforeEach
    public void setup() {
        consumerNetworkThread.initializeResources();
    }

    @AfterEach
    public void tearDown() {
        if (consumerNetworkThread != null)
            consumerNetworkThread.close();
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 100, 1000, 4999, 5001})
    public void testConsumerNetworkThreadWaitTimeComputations(long exampleTime) {
        List<Optional<? extends RequestManager>> requestManagersList = new ArrayList<>();
        requestManagersList.add(Optional.of(coordinatorRequestManager));
        when(requestManagers.entries()).thenReturn(requestManagersList);

        NetworkClientDelegate.PollResult pollResult = new NetworkClientDelegate.PollResult(exampleTime);

        when(coordinatorRequestManager.poll(anyLong())).thenReturn(pollResult);
        when(coordinatorRequestManager.maximumTimeToWait(anyLong())).thenReturn(exampleTime);
        when(networkClientDelegate.addAll(pollResult)).thenReturn(pollResult.timeUntilNextPollMs);
        consumerNetworkThread.runOnce();

        // verify networkClientDelegate polls with the correct time
        assertEquals(consumerNetworkThread.maximumTimeToWait(), exampleTime);
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        consumerNetworkThread.start();
        TestCondition isStarted = () -> consumerNetworkThread.isRunning();
        TestCondition isClosed = () -> !(consumerNetworkThread.isRunning() || consumerNetworkThread.isAlive());

        // There's a nonzero amount of time between starting the thread and having it
        // begin to execute our code. Wait for a bit before checking...
        TestUtils.waitForCondition(isStarted,
                "The consumer network thread did not start within " + DEFAULT_MAX_WAIT_MS + " ms");

        consumerNetworkThread.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));

        TestUtils.waitForCondition(isClosed,
                "The consumer network thread did not stop within " + DEFAULT_MAX_WAIT_MS + " ms");
    }

    @Test
    void testRequestManagersArePolledOnce() {
        consumerNetworkThread.runOnce();
        requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).poll(anyLong())));
        requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).maximumTimeToWait(anyLong())));
        verify(networkClientDelegate).poll(anyLong(), anyLong());
    }

    @Test
    public void testApplicationEvent() {
        ApplicationEvent e = new PollEvent(100);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(e);
    }

    @Test
    public void testMetadataUpdateEvent() {
        ApplicationEvent e = new NewTopicsMetadataUpdateRequestEvent();
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(e);
    }

    @Test
    public void testAsyncCommitEvent() {
        ApplicationEvent e = new AsyncCommitEvent(new HashMap<>());
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(AsyncCommitEvent.class));
    }

    @Test
    public void testSyncCommitEvent() {
        ApplicationEvent e = new SyncCommitEvent(new HashMap<>(), calculateDeadlineMs(time, 100));
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(SyncCommitEvent.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testListOffsetsEventIsProcessed(boolean requireTimestamp) {
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        ApplicationEvent e = new ListOffsetsEvent(timestamps, calculateDeadlineMs(time, 100), requireTimestamp);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(ListOffsetsEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testResetPositionsEventIsProcessed() {
        ResetPositionsEvent e = new ResetPositionsEvent(calculateDeadlineMs(time, 100));
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(ResetPositionsEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testResetPositionsProcessFailureIsIgnored() {
        doThrow(new NullPointerException()).when(offsetsRequestManager).resetPositionsIfNeeded();

        ResetPositionsEvent event = new ResetPositionsEvent(calculateDeadlineMs(time, 100));
        applicationEventsQueue.add(event);
        assertDoesNotThrow(() -> consumerNetworkThread.runOnce());

        verify(applicationEventProcessor).process(any(ResetPositionsEvent.class));
    }

    @Test
    public void testValidatePositionsEventIsProcessed() {
        ValidatePositionsEvent e = new ValidatePositionsEvent(calculateDeadlineMs(time, 100));
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(ValidatePositionsEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    // Look into this one
    @Test
    public void testAssignmentChangeEvent() {
        HashMap<TopicPartition, OffsetAndMetadata> offset = mockTopicPartitionOffset();

        final long currentTimeMs = time.milliseconds();
        ApplicationEvent e = new AssignmentChangeEvent(offset, currentTimeMs);
        applicationEventsQueue.add(e);

        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(AssignmentChangeEvent.class));
        verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
        verify(commitRequestManager, times(1)).updateAutoCommitTimer(currentTimeMs);
        // Assignment change should generate an async commit (not retried).
        verify(commitRequestManager, times(1)).maybeAutoCommitAsync();
    }

    @Test
    void testFetchTopicMetadata() {
        applicationEventsQueue.add(new TopicMetadataEvent("topic", Long.MAX_VALUE));
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(TopicMetadataEvent.class));
    }

    // Look into this one, may need to use networkClient instead of the delegate
    @Test
    void testPollResultTimer() {
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
                Optional.empty());
        req.setTimer(time, DEFAULT_REQUEST_TIMEOUT_MS);

        // purposely setting a non-MAX time to ensure it is returning Long.MAX_VALUE upon success
        NetworkClientDelegate.PollResult success = new NetworkClientDelegate.PollResult(
                10,
                Collections.singletonList(req));
        assertEquals(10, networkClientDelegate.addAll(success));

        NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                10,
                new ArrayList<>());
        assertEquals(10, networkClientDelegate.addAll(failure));
    }

    // Should work, look into this
    @Test
    void testMaximumTimeToWait() {
        // Initial value before runOnce has been called
        assertEquals(ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, consumerNetworkThread.maximumTimeToWait());
        consumerNetworkThread.runOnce();
        // After runOnce has been called, it takes the default heartbeat interval from the heartbeat request manager
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, consumerNetworkThread.maximumTimeToWait());
    }

    // Check on this one
    @Test
    void testEnsureMetadataUpdateOnPoll() {
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(2, Collections.emptyMap());
        client.prepareMetadataUpdate(metadataResponse);
        metadata.requestUpdate(false);
        consumerNetworkThread.runOnce();
        verify(metadata, times(1)).updateWithCurrentRequestVersion(eq(metadataResponse), eq(false), anyLong());
    }

    // Look into this one
    @Test
    void testEnsureEventsAreCompleted() {
        // Mimic the logic of CompletableEventReaper.reap(Collection):
        doAnswer(__ -> {
            Iterator<ApplicationEvent> i = applicationEventsQueue.iterator();

            while (i.hasNext()) {
                ApplicationEvent event = i.next();

                if (event instanceof CompletableEvent)
                    ((CompletableEvent<?>) event).future().completeExceptionally(new TimeoutException());

                i.remove();
            }

            return null;
        }).when(applicationEventReaper).reap(any(Collection.class));

        Node node = metadata.fetch().nodes().get(0);
        coordinatorRequestManager.markCoordinatorUnknown("test", time.milliseconds());
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "group-id", node));
        prepareOffsetCommitRequest(new HashMap<>(), Errors.NONE, false);
        CompletableApplicationEvent<Void> event1 = spy(new AsyncCommitEvent(Collections.emptyMap()));
        ApplicationEvent event2 = new AsyncCommitEvent(Collections.emptyMap());
        CompletableFuture<Void> future = new CompletableFuture<>();
        when(event1.future()).thenReturn(future);
        applicationEventsQueue.add(event1);
        applicationEventsQueue.add(event2);
        assertFalse(future.isDone());
        assertFalse(applicationEventsQueue.isEmpty());
        consumerNetworkThread.cleanup();
        assertTrue(future.isCompletedExceptionally());
        assertTrue(applicationEventsQueue.isEmpty());
    }

    // Look into this one
    @Test
    void testCleanupInvokesReaper() {
        consumerNetworkThread.cleanup();
        verify(applicationEventReaper).reap(applicationEventsQueue);
    }

    @Test
    void testRunOnceInvokesReaper() {
        consumerNetworkThread.runOnce();
        verify(applicationEventReaper).reap(any(Long.class));
    }

    private HashMap<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private void prepareOffsetCommitRequest(final Map<TopicPartition, Long> expectedOffsets,
                                            final Errors error,
                                            final boolean disconnected) {
        Map<TopicPartition, Errors> errors = partitionErrors(expectedOffsets.keySet(), error);
        client.prepareResponse(offsetCommitRequestMatcher(expectedOffsets), offsetCommitResponse(errors), disconnected);
    }

    private Map<TopicPartition, Errors> partitionErrors(final Collection<TopicPartition> partitions,
                                                        final Errors error) {
        final Map<TopicPartition, Errors> errors = new HashMap<>();
        for (TopicPartition partition : partitions) {
            errors.put(partition, error);
        }
        return errors;
    }

    private OffsetCommitResponse offsetCommitResponse(final Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    private MockClient.RequestMatcher offsetCommitRequestMatcher(final Map<TopicPartition, Long> expectedOffsets) {
        return body -> {
            OffsetCommitRequest req = (OffsetCommitRequest) body;
            Map<TopicPartition, Long> offsets = req.offsets();
            if (offsets.size() != expectedOffsets.size())
                return false;

            for (Map.Entry<TopicPartition, Long> expectedOffset : expectedOffsets.entrySet()) {
                if (!offsets.containsKey(expectedOffset.getKey())) {
                    return false;
                } else {
                    Long actualOffset = offsets.get(expectedOffset.getKey());
                    if (!actualOffset.equals(expectedOffset.getValue())) {
                        return false;
                    }
                }
            }
            return true;
        };
    }
}