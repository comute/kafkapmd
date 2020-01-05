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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ChangelogReader is created and maintained by the stream thread and used for both updating standby tasks and
 * restoring active tasks. It manages the restore consumer, including its assigned partitions, when to pause / resume
 * these partitions, etc.
 *
 * The reader also maintains the source of truth for restoration state: only active tasks restoring changelog could
 * be completed, while standby tasks updating changelog would always be in restoring state after being initialized.
 */
public class StoreChangelogReader implements ChangelogReader {

    private static final long UNKNOWN_OFFSET = -1L;

    enum ChangelogState {
        // registered but need to be initialized (i.e. set its starting, end, limit offsets)
        REGISTERED("REGISTERED"),

        // initialized and restoring
        RESTORING("RESTORING"),

        // completed restoring (only for active restoring task, standby task should never be completed)
        COMPLETED("COMPLETED");

        // the restore consumer should contain all changelogs that are RESTORING or COMPLETED

        public final String name;

        ChangelogState(final String name) {
            this.name = name;
        }
    }

    // NOTE we assume that the changelog read is used only for either 1) restoring active task
    // or 2) updating standby task at a given time, but never doing both
    enum ChangelogReaderState {
        ACTIVE_RESTORING("ACTIVE_RESTORING"),

        STANDBY_UPDATING("STANDBY_UPDATING");

        public final String name;

        ChangelogReaderState(final String name) {
            this.name = name;
        }
    }

    private class ChangelogMetadata {

        private final TopicPartition changelogPartition;

        private final ProcessorStateManager stateManager;

        private ChangelogState changelogState;

        private long totalRestored;

        // only for active restoring tasks (for standby it is null)
        private Long restoreEndOffset;

        // only for standby tasks that use source topics as changelogs (for active it is null);
        // if it is not on source topics it is also null
        private Long restoreLimitOffset;

        // only for standby tasks to buffer records polled but exceed limit at the moment;
        // for active it should be null
        private List<ConsumerRecord<byte[], byte[]>> bufferedRecords;

        // NOTE we do not book keep the current offset since we leverage state manager as its source of truth

        private ChangelogMetadata(final TopicPartition changelogPartition, final ProcessorStateManager stateManager) {
            final boolean shouldUpdateLimitOffset = stateManager.changelogAsSource(changelogPartition);

            this.stateManager = stateManager;
            this.changelogPartition = changelogPartition;
            this.changelogState = ChangelogState.REGISTERED;
            this.restoreEndOffset = null;
            this.totalRestored = 0L;

            if (shouldUpdateLimitOffset) {
                this.bufferedRecords = new ArrayList<>();
                this.restoreLimitOffset = 0L;
            } else {
                this.bufferedRecords = null;
                this.restoreLimitOffset = null;
            }
        }

        private void clear() {
            if (this.bufferedRecords != null) {
                this.bufferedRecords.clear();
                this.bufferedRecords = null;
            }
        }

        @Override
        public String toString() {
            final Long currentOffset = stateManager.changelogOffsets().get(changelogPartition);
            return changelogState + " " + stateManager.taskType() +
                " (currentOffset + "+ currentOffset + "endOffset " + restoreEndOffset + ", limitOffset " + restoreLimitOffset + ")";
        }
    }

    private ChangelogReaderState state;

    private final Logger log;
    private final Duration pollTime;

    // 1) we keep adding partitions to restore consumer whenever new tasks are registered with the state manager;
    // 2) we do not unassign partitions when we switch between standbys and actives, we just pause / resume them;
    // 3) we only remove an assigned partition when the corresponding task is being removed from the thread.
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final StateRestoreListener stateRestoreListener;

    // source of the truth of the current registered changelogs;
    // NOTE a changelog would only be removed when its corresponding task
    // is being removed from the thread; otherwise it would stay in this map even after completed
    private final Map<TopicPartition, ChangelogMetadata> changelogs;

    // if some partition metadata does not exist yet then it is highly likely that fetching
    // for their end offsets would timeout; thus we maintain a global partition information
    // and only try to fetch for the corresponding end offset after we know that this
    // partition already exists from the broker-side metadata: this is an optimization.
    private final Map<String, List<PartitionInfo>> partitionInfo = new HashMap<>();

    // the changelog reader only need the main consumer to get committed offsets for source changelog partitions
    // to update offset limit for standby tasks;
    private Consumer<byte[], byte[]> mainConsumer;

    void setMainConsumer(final Consumer<byte[], byte[]> consumer) {
        this.mainConsumer = consumer;
    }

    public StoreChangelogReader(final StreamsConfig config,
                                final LogContext logContext,
                                final Consumer<byte[], byte[]> restoreConsumer,
                                final StateRestoreListener stateRestoreListener) {
        this.log = logContext.logger(StoreChangelogReader.class);
        this.state = ChangelogReaderState.ACTIVE_RESTORING;
        this.restoreConsumer = restoreConsumer;
        this.stateRestoreListener = stateRestoreListener;

        // NOTE for restoring active and updating standby we may prefer different poll time
        // in order to make sure we call the main consumer#poll in time.
        // TODO: once both of these are moved to a separate thread this may no longer be a concern
        this.pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));

        this.changelogs = new HashMap<>();
    }

    private static String recordEndOffset(final Long endOffset) {
        return endOffset == null ? "UNKNOWN (since it is for standby task)" : endOffset.toString();
    }

    private static boolean restoredToEnd(final Long endOffset, final Long currentOffset) {
        if (endOffset == null) {
            // end offset is not initialized meaning that it is from a standby task (i.e. it should never end)
            return false;
        } else if (endOffset == 0) {
            // this is a special case, meaning there's nothing to be restored since the changelog has no data
            // of the changelog is a source topic and there's no committed offset
            return true;
        } else if (currentOffset == null) {
            // current offset is not initialized meaning there's no checkpointed offset,
            // we would start restoring from beginning and it does not end yet
            return false;
        } else {
            // note the end offset returned of the consumer is actually the last offset + 1
            return currentOffset + 1 < endOffset;
        }
    }

    // Once some new tasks are created, we transit to restore them and pause on the existing standby tasks. It is
    // possible that when newly created tasks are created the thread and its changelog reader are still restoring existing
    // active tasks, and hence this function is idempotent and can be called multiple times.
    //
    // NOTE: even if the newly created tasks do not need any restoring, we still first transit to this state and then
    // immediately transit back -- there's no overhead of transiting back and forth but simplifies the logic a lot.
    // TODO K9113: this function should be called by stream thread
    public void transitToRestoreActive() {
        log.debug("Transiting to restore active tasks: {}", changelogs);

        // pause all partitions that are for standby tasks from the restore consumer
        pauseChangelogsFromRestoreConsumer(standbyRestoringChangelogs());

        state = ChangelogReaderState.ACTIVE_RESTORING;
    }

    // Only after we've completed restoring all active tasks we'll then move back to resume updating standby tasks.
    // This function is NOT idempotent: if it is already in updating standby tasks mode, we should not call it again.
    //
    // NOTE: we do not clear completed active restoring changelogs or remove partitions from restore consumer either
    // upon completing them but only pause the corresponding partitions; the changelog metadata / partitions would only
    // be cleared when the corresponding task is being removed from the thread.
    // TODO K9113: this function should be called by stream thread
    public void transitToUpdateStandby() {
        if (state != ChangelogReaderState.ACTIVE_RESTORING) {
            throw new IllegalStateException("The changelog reader is not restoring actove tasks while trying to " +
                "transit to update standby tasks");
        }

        log.debug("Transiting to update standby tasks: {}", changelogs);

        // resume all standby restoring changelogs from the restore consumer
        resumeChangelogsFromRestoreConsumer(standbyRestoringChangelogs());

        state = ChangelogReaderState.STANDBY_UPDATING;
    }

    /**
     * Since it is shared for multiple tasks and hence multiple state managers, the registration would take its
     * corresponding state manager as well for restoring.
     */
    @Override
    public void register(final TopicPartition partition, final ProcessorStateManager stateManager) {
        final ChangelogMetadata changelogMetadata = new ChangelogMetadata(partition, stateManager);

        if (state == ChangelogReaderState.STANDBY_UPDATING && stateManager.changelogAsSource(partition)) {
            // set it to an not-null value indicating it needs to be updated
            changelogMetadata.restoreLimitOffset = UNKNOWN_OFFSET;
        }

        if (changelogs.putIfAbsent(partition, changelogMetadata) != null) {
            throw new IllegalStateException("There is already a changelog registered for " + partition +
                ", this should not happen.");
        }
    }

    private Set<ChangelogMetadata> registeredChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.REGISTERED)
            .collect(Collectors.toSet());
    }

    private Set<TopicPartition> restoringChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.RESTORING)
            .map(metadata -> metadata.changelogPartition)
            .collect(Collectors.toSet());
    }

    private Set<TopicPartition> standbyRestoringChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.RESTORING &&
                metadata.stateManager.taskType() == AbstractTask.TaskType.STANDBY)
            .map(metadata -> metadata.changelogPartition)
            .collect(Collectors.toSet());
    }

    private boolean allChangelogsCompleted() {
        return changelogs.values().stream()
            .allMatch(metadata -> metadata.changelogState == ChangelogState.COMPLETED);
    }

    // for stream thread to decide to transit to normal processing
    boolean allActiveChangelogsCompleted() {
        for (final ChangelogMetadata metadata : changelogs.values()) {
            if (metadata.stateManager.taskType() == AbstractTask.TaskType.ACTIVE) {
                if (metadata.changelogState != ChangelogState.COMPLETED)
                    return false;
            }
        }
        return true;
    }

    @Override
    public Set<TopicPartition> completedChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.COMPLETED)
            .map(metadata -> metadata.changelogPartition)
            .collect(Collectors.toSet());
    }

    // 1. if there are any registered changelogs that needs initialization, try to initialize them first;
    // 2. if all changelogs have finished, return early;
    // 3. if there are any restoring changelogs, try to read from the restore consumer and process them.
    public void restore() {
        initializeChangelogs(registeredChangelogs());

        if (allChangelogsCompleted()) {
            log.info("Finished restoring all changelogs {}", changelogs.keySet());
            return;
        }

        if (!restoringChangelogs().isEmpty()) {
            final ConsumerRecords<byte[], byte[]> polledRecords = restoreConsumer.poll(pollTime);

            for (final TopicPartition partition : polledRecords.partitions()) {
                final ChangelogMetadata changelogMetadata = changelogs.get(partition);
                if (changelogMetadata == null) {
                    throw new IllegalStateException("The corresponding changelog restorer for " + partition +
                        " does not exist, this should not happen.");
                }
                if (changelogMetadata.changelogState != ChangelogState.RESTORING) {
                    throw new IllegalStateException("The corresponding changelog restorer for " + partition +
                        " has already transited to completed state, this should not happen.");
                }

                final List<ConsumerRecord<byte[], byte[]>> records = polledRecords.records(partition);
                final Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    final ConsumerRecord<byte[], byte[]> record = iterator.next();
                    if (record.key() == null) {
                        log.warn("Read changelog record with null key from changelog {} at offset {}, " +
                            "skipping it for restoration", changelogMetadata.changelogPartition, record.offset());
                        iterator.remove();
                    }
                }

                // TODO: we always try to restore as a batch when some records are accumulated, which may result in
                //       small batches; this can be optimized in the future, e.g. wait longer for larger batches.
                if (!records.isEmpty()) {
                    if (changelogMetadata.bufferedRecords != null && !changelogMetadata.bufferedRecords.isEmpty()) {
                        changelogMetadata.bufferedRecords.addAll(records);
                    } else {
                        changelogMetadata.bufferedRecords = records;
                    }

                    restoreChangelog(changelogMetadata);
                }
            }
        }
    }

    /**
     * returns a flag whether offset limit caused not all records restored
     */
    private void restoreChangelog(final ChangelogMetadata changelogMetadata) {
        final TopicPartition partition = changelogMetadata.changelogPartition;
        final int numRecords = IntStream.range(0, changelogMetadata.bufferedRecords.size())
            .filter(i -> {
                final ConsumerRecord<byte[], byte[]> record = changelogMetadata.bufferedRecords.get(i);
                final long offset = record.offset();

                return changelogMetadata.restoreEndOffset != null && offset >= changelogMetadata.restoreEndOffset ||
                    changelogMetadata.restoreLimitOffset != null && offset >= changelogMetadata.restoreLimitOffset;
            }).findFirst().orElse(changelogMetadata.bufferedRecords.size());

        if (numRecords != 0) {
            final ProcessorStateManager stateManager = changelogMetadata.stateManager;
            final List<ConsumerRecord<byte[], byte[]>> restoreRecords = changelogMetadata.bufferedRecords.subList(0, numRecords);
            stateManager.restore(partition, restoreRecords);

            final StateStoreMetadata storeMetadata = stateManager.storeMetadata(partition);
            final String storeName = storeMetadata.stateStore.name();
            final Long currentOffset = storeMetadata.offset;
            changelogMetadata.totalRestored += numRecords;

            log.trace("Restored {} records from changelog {} to store {}, end offset is {}, current offset is {}",
                partition, storeName, numRecords, recordEndOffset(changelogMetadata.restoreEndOffset), currentOffset);

            // do not trigger restore listener if we are processing standby tasks, also we do not need to check
            // if it has been completed since standby task never completes
            if (changelogMetadata.stateManager.taskType() == AbstractTask.TaskType.ACTIVE) {
                stateRestoreListener.onBatchRestored(partition, storeName, currentOffset, numRecords);

                if (restoredToEnd(changelogMetadata.restoreEndOffset, currentOffset)) {
                    log.info("Finished restoring changelog {} to store {} with a total number of {} records",
                        partition, storeName, changelogMetadata.totalRestored);

                    stateRestoreListener.onRestoreEnd(partition, storeName, changelogMetadata.totalRestored);

                    pauseChangelogsFromRestoreConsumer(Collections.singleton(changelogMetadata.changelogPartition));
                }
            }

            // NOTE we use removeAll of ArrayList in order to achieve efficiency, otherwise one-at-a-time removal
            // would be very costly as it shifts element each time.
            if (numRecords < changelogMetadata.bufferedRecords.size()) {
                changelogMetadata.bufferedRecords.removeAll(restoreRecords);
            } else {
                changelogMetadata.bufferedRecords.clear();
                changelogMetadata.bufferedRecords = null;
            }
        }
    }

    private Map<TopicPartition, Long> committedOffsetForChangelogs(final Set<TopicPartition> partitions) {
        try {
            // those do not have a committed offset would default to 0
            return mainConsumer.committed(partitions).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() == null ? 0L : e.getValue().offset()));
        } catch (final TimeoutException e) {
            // if it timed out we just retry next time.
            return Collections.emptyMap();
        } catch (final KafkaException e) {
            throw new StreamsException(String.format("Failed to retrieve end offsets for %s", partitions), e);
        }
    }

    private Map<TopicPartition, Long> endOffsetForChangelogs(final Set<TopicPartition> partitions) {
        try {
            return restoreConsumer.endOffsets(partitions);
        } catch (final TimeoutException e) {
            // if timeout exception gets thrown we just give up this time and retry in the next run loop
            log.debug("Could not fetch all end offsets for {}, will retry in the next run loop", partitions);
            return Collections.emptyMap();
        } catch (final KafkaException e) {
            throw new StreamsException(String.format("Failed to retrieve end offsets for %s", partitions), e);
        }
    }

    // TODO K9113: standby task that have source changelogs should call this function periodically
    public void updateLimitOffsets() {
        if (state != ChangelogReaderState.STANDBY_UPDATING) {
            throw new IllegalStateException("We should not try to update standby tasks limit offsets if there are still" +
                " active tasks for restoring");
        }

        final Set<TopicPartition> changelogsWithLimitOffsets = changelogs.entrySet().stream()
            .filter(entry -> entry.getValue().stateManager.taskType() == AbstractTask.TaskType.STANDBY &&
                entry.getValue().stateManager.changelogAsSource(entry.getKey()))
            .map(Map.Entry::getKey).collect(Collectors.toSet());

        updateLimitOffsetsForStandbyChangelogs(committedOffsetForChangelogs(changelogsWithLimitOffsets));
    }

    private void updateLimitOffsetsForStandbyChangelogs(final Map<TopicPartition, Long> committedOffsets) {
        for (final ChangelogMetadata metadata : changelogs.values()) {
            if (metadata.stateManager.taskType() == AbstractTask.TaskType.STANDBY &&
                metadata.stateManager.changelogAsSource(metadata.changelogPartition) &&
                committedOffsets.containsKey(metadata.changelogPartition)) {

                final Long newLimit = committedOffsets.get(metadata.changelogPartition);
                final Long previousLimit = metadata.restoreLimitOffset;

                if (previousLimit != null && previousLimit > newLimit) {
                    throw new IllegalStateException("Offset limit should monotonically increase, but was reduced for partition " +
                        metadata.changelogPartition + ". New limit: " + newLimit + ". Previous limit: " + previousLimit);
                }

                metadata.restoreLimitOffset = newLimit;
            }
        }
    }

    private void initializeChangelogs(final Set<ChangelogMetadata> newPartitionsToRestore) {
        if (newPartitionsToRestore.isEmpty())
            return;

        // for active changelogs, we need to find their end offset before transit to restoring
        // if the changelog is on source topic, then its end offset should be the minimum of
        // its committed offset and its end offset; for standby tasks that use source topics
        // as changelogs, we want to initialize their limit offsets as committed offsets as well
        final Set<TopicPartition> newPartitionsToFindEndOffset = new HashSet<>();
        final Set<TopicPartition> newPartitionsToFindCommittedOffset = new HashSet<>();

        for (final ChangelogMetadata metadata : newPartitionsToRestore) {
            final TopicPartition partition = metadata.changelogPartition;

            if (metadata.stateManager.taskType() == AbstractTask.TaskType.ACTIVE) {
                newPartitionsToFindEndOffset.add(partition);
                if (metadata.stateManager.changelogAsSource(partition))
                    newPartitionsToFindCommittedOffset.add(partition);
            } else if (metadata.stateManager.taskType() == AbstractTask.TaskType.STANDBY) {
                if (metadata.stateManager.changelogAsSource(partition))
                    newPartitionsToFindCommittedOffset.add(partition);
            }
        }

        // NOTE we assume that all requested partitions will be included in the returned map for both end/committed
        // offsets, i.e., it would not return partial result and would timeout if some of the results cannot be found
        final Map<TopicPartition, Long> endOffsets = newPartitionsToFindEndOffset.isEmpty() ?
            Collections.emptyMap() : endOffsetForChangelogs(newPartitionsToFindEndOffset);
        final Map<TopicPartition, Long> committedOffsets = newPartitionsToFindCommittedOffset.isEmpty() ?
            Collections.emptyMap() : committedOffsetForChangelogs(newPartitionsToFindCommittedOffset);

        for (final TopicPartition partition: newPartitionsToFindEndOffset) {
            final ChangelogMetadata changelogMetadata = changelogs.get(partition);
            final Long endOffset = endOffsets.get(partition);
            final Long committedOffset = newPartitionsToFindCommittedOffset.contains(partition) ?
                committedOffsets.get(partition) : Long.MAX_VALUE;

            if (endOffset != null && committedOffset != null) {
                changelogMetadata.restoreEndOffset = Math.min(endOffset, committedOffset);

                log.debug("End offset for changelog {} initialized as {}.", partition, changelogMetadata.restoreEndOffset);
            } else {
                if (!newPartitionsToRestore.remove(changelogMetadata))
                    throw new IllegalStateException("New changelogs to restore " + newPartitionsToRestore +
                        " does not contain the one looking for end offset: " + partition + ", this should not happen.");

                log.info("End offset for changelog {} cannot be found; will retry in the next time.", partition);
            }
        }

        // try initialize limit offsets for standby tasks for the first time
        if (!committedOffsets.isEmpty())
            updateLimitOffsetsForStandbyChangelogs(committedOffsets);

        // add new partitions to the restore consumer and transit them to restoring state
        addChangelogsToRestoreConsumer(newPartitionsToRestore.stream().map(metadata -> metadata.changelogPartition)
            .collect(Collectors.toSet()));

        // if it is in the active restoring mode, we immediately pause those standby changelogs
        // here we just blindly pause all (including the existing and newly added)
        if (state == ChangelogReaderState.ACTIVE_RESTORING) {
            pauseChangelogsFromRestoreConsumer(standbyRestoringChangelogs());
        }

        newPartitionsToRestore.forEach(metadata -> metadata.changelogState = ChangelogState.RESTORING);

        // prepare newly added partitions of the restore consumer by setting their starting position
        prepareChangelogs(newPartitionsToRestore);
    }

    private void addChangelogsToRestoreConsumer(final Set<TopicPartition> partitions) {
        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());

        // the current assignment should not contain any of the new partitions
        if (assignment.removeAll(partitions)) {
            throw new IllegalStateException("The current assignment " + assignment + " " +
                "already contains some of the new partitions " + partitions);
        }
        assignment.addAll(partitions);
        restoreConsumer.assign(assignment);
    }

    private void pauseChangelogsFromRestoreConsumer(final Collection<TopicPartition> partitions) {
        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());

        // the current assignment should contain the all partitions to pause
        if (!assignment.containsAll(partitions)) {
            throw new IllegalStateException("The current assignment " + assignment + " " +
                "does not contain some of the partitions " + partitions + " for pausing.");
        }
        restoreConsumer.pause(partitions);
    }

    private void removeChangelogsFromRestoreConsumer(final Collection<TopicPartition> partitions) {
        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());

        // the current assignment should contain the all partitions to remove
        if (!assignment.containsAll(partitions)) {
            throw new IllegalStateException("The current assignment " + assignment + " " +
                "does not contain some of the partitions " + partitions + " for removing.");
        }
        assignment.removeAll(partitions);
        restoreConsumer.assign(assignment);
    }

    private void resumeChangelogsFromRestoreConsumer(final Collection<TopicPartition> partitions) {
        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());

        // the current assignment should contain the all partitions to resume
        if (!assignment.containsAll(partitions)) {
            throw new IllegalStateException("The current assignment " + assignment + " " +
                "does not contain some of the partitions " + partitions + " for resuming.");
        }
        restoreConsumer.resume(partitions);
    }

    private void prepareChangelogs(final Set<ChangelogMetadata> newPartitionsToRestore) {
        // separate those who do not have the current offset loaded from checkpoint
        final Set<TopicPartition> newPartitionsWithoutStartOffset = new HashSet<>();

        for (final ChangelogMetadata changelogMetadata : newPartitionsToRestore) {
            final TopicPartition partition = changelogMetadata.changelogPartition;
            final StateStoreMetadata storeMetadata = changelogMetadata.stateManager.storeMetadata(partition);
            final Long currentOffset = storeMetadata.offset;
            final Long endOffset = changelogs.get(partition).restoreEndOffset;

            if (currentOffset != null) {
                restoreConsumer.seek(partition, currentOffset);

                log.debug("Start restoring changelog partition {} from current offset {} to end offset {}.",
                    partition, currentOffset, recordEndOffset(endOffset));
            } else {
                log.debug("Start restoring changelog partition {} from the beginning offset {} to end offset {} " +
                    "since we cannot find current offset.", partition, recordEndOffset(endOffset));

                newPartitionsWithoutStartOffset.add(partition);
            }

            // we do not trigger restore listener for standby tasks (whose end offset is unknown)
            if (changelogMetadata.stateManager.taskType() == AbstractTask.TaskType.ACTIVE) {
                // NOTE if there's no offset to seek, i.e. we would seek from the beginning, we trigger callback with 0L
                final long startOffset = currentOffset == null ? 0L : currentOffset;
                stateRestoreListener.onRestoreStart(partition, storeMetadata.stateStore.name(), startOffset, endOffset);
            }
        }

        // optimization: batch all seek-to-beginning offsets in a single request
        if (!newPartitionsWithoutStartOffset.isEmpty()) {
            restoreConsumer.seekToBeginning(newPartitionsWithoutStartOffset);
        }
    }

    @Override
    // TODO K9113: when a task is removed from the thread, this should be called
    public void remove(final List<TopicPartition> revokedChangelogs) {
        for (final TopicPartition partition : revokedChangelogs) {
            ChangelogMetadata changelogMetadata = changelogs.remove(partition);
            changelogMetadata.clear();
        }

        removeChangelogsFromRestoreConsumer(revokedChangelogs);
    }

    @Override
    public void clear() {
        for (final ChangelogMetadata changelogMetadata : changelogs.values()) {
            changelogMetadata.clear();
        }
        changelogs.clear();

        restoreConsumer.unsubscribe();
    }

    @Override
    public boolean isEmpty() {
        return changelogs.isEmpty();
    }

    @Override
    public String toString() {
        return "StoreChangelogReader: " + changelogs + "\n";
    }

    // for testing only
    public Map<TopicPartition, Long> restoredOffsets() {
        final Map<TopicPartition, Long> restoredOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, ChangelogMetadata> entry : changelogs.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final ChangelogMetadata changelogMetadata = entry.getValue();
            final boolean storeIsPersistent = changelogMetadata.stateManager.storeMetadata(partition).stateStore.persistent();

            if (storeIsPersistent) {
                restoredOffsets.put(entry.getKey(), changelogMetadata.stateManager.storeMetadata(partition).offset);
            }
        }
        return restoredOffsets;
    }
}
