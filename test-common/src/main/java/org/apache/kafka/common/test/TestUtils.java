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
package org.apache.kafka.common.test;

import kafka.log.UnifiedLog;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

import static java.lang.String.format;

/**
 * Helper functions for writing unit tests
 */
public class TestUtils {
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    private static final long DEFAULT_POLL_INTERVAL_MS = 100;
    private static final long DEFAULT_MAX_WAIT_MS = 15_000;
    private static final long DEFAULT_TIMEOUT_MS = 60_000;

    /**
     * Create an empty file in the default temporary-file directory, using `kafka` as the prefix and `tmp` as the
     * suffix to generate its name.
     */
    public static File tempFile() throws IOException {
        final File file = Files.createTempFile("kafka", ".tmp").toFile();
        file.deleteOnExit();
        return file;
    }

    /**
     * Create a temporary relative directory in the specified parent directory with the given prefix.
     *
     */
    static File tempDirectory() {
        final File file;
        String prefix = "kafka-";
        try {
            file = Files.createTempDirectory(prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }

        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                log.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        });

        return file;
    }

    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final Supplier<Boolean> testCondition, final String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, conditionDetails);
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final Supplier<Boolean> testCondition, 
                                        final long maxWaitMs, 
                                        String conditionDetails
    ) throws InterruptedException {
        retryOnExceptionWithTimeout(() -> {
            String conditionDetail = conditionDetails == null ? "" : conditionDetails;
            if (!testCondition.get())
                throw new TimeoutException("Condition not met within timeout " + maxWaitMs + ". " + conditionDetail);
        });
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the given timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    static void retryOnExceptionWithTimeout(final Runnable runnable) throws InterruptedException {
        final long expectedEnd = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        while (true) {
            try {
                runnable.run();
                return;
            } catch (final AssertionError t) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw t;
                }
            } catch (final Exception e) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw new AssertionError(format("Assertion failed with an exception after %s ms", DEFAULT_TIMEOUT_MS), e);
                }
            }
            Thread.sleep(DEFAULT_POLL_INTERVAL_MS);
        }
    }

    /**
     * Wrap a single record log buffer.
     */
    public static MemoryRecords singletonRecords(byte[] value,
                                                 byte[] key,
                                                 Compression codec,
                                                 long timestamp,
                                                 byte magicValue) {
        return records(Collections.singletonList(new SimpleRecord(timestamp, key, value)), magicValue, codec);
    }

    public static MemoryRecords singletonRecords(byte[] value, byte[] key) {
        return singletonRecords(value, key, Compression.NONE, RecordBatch.NO_TIMESTAMP, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords records(List<SimpleRecord> records,
                                        byte magicValue,
                                        Compression codec,
                                        long producerId,
                                        short producerEpoch,
                                        int sequence,
                                        long baseOffset,
                                        int partitionLeaderEpoch) {
        int sizeInBytes = DefaultRecordBatch.sizeInBytes(records);
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer,
            magicValue,
            codec,
            TimestampType.CREATE_TIME,
            baseOffset,
            System.currentTimeMillis(),
            producerId,
            producerEpoch,
            sequence,
            false,
            partitionLeaderEpoch
        )) {
            records.forEach(builder::append);
            return builder.build();
        }
    }

    public static MemoryRecords records(List<SimpleRecord> records, byte magicValue, Compression codec) {
        return records(records,
            magicValue,
            codec,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            0L,
            RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static <B extends KafkaBroker> void verifyTopicDeletion(String topic,
                                                                   int numPartitions,
                                                                   Collection<B> brokers) throws Exception {
        List<TopicPartition> topicPartitions = IntStream.range(0, numPartitions)
            .mapToObj(partition -> new TopicPartition(topic, partition))
            .collect(Collectors.toList());

        // Ensure that the topic-partition has been deleted from all brokers' replica managers
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                topicPartitions.stream().allMatch(tp -> broker.replicaManager().onlinePartition(tp).isEmpty())),
            "Replica manager's should have deleted all of this topic's partitions");

        // Ensure that logs from all replicas are deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                topicPartitions.stream().allMatch(tp -> broker.logManager().getLog(tp, false).isEmpty())),
            "Replica logs not deleted after delete topic is complete");

        // Ensure that the topic is removed from all cleaner offsets
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                topicPartitions.stream().allMatch(tp -> {
                    List<File> liveLogDirs = CollectionConverters.asJava(broker.logManager().liveLogDirs());
                    return liveLogDirs.stream().allMatch(logDir -> {
                        OffsetCheckpointFile checkpointFile;
                        try {
                            checkpointFile = new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return !checkpointFile.read().containsKey(tp);
                    });
                })),
            "Cleaner offset for deleted partition should have been removed");

        // Ensure that the topic directories are soft-deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                CollectionConverters.asJava(broker.config().logDirs()).stream().allMatch(logDir ->
                    topicPartitions.stream().noneMatch(tp ->
                        new File(logDir, tp.topic() + "-" + tp.partition()).exists()))),
            "Failed to soft-delete the data to a delete directory");

        // Ensure that the topic directories are hard-deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
            CollectionConverters.asJava(broker.config().logDirs()).stream().allMatch(logDir ->
                topicPartitions.stream().allMatch(tp ->
                    Arrays.stream(Objects.requireNonNull(new File(logDir).list())).noneMatch(partitionDirectoryName ->
                        partitionDirectoryName.startsWith(tp.topic() + "-" + tp.partition()) &&
                            partitionDirectoryName.endsWith(UnifiedLog.DeleteDirSuffix())))
            )
        ), "Failed to hard-delete the delete directory");
    }

    public static int waitUntilLeaderIsElectedOrChangedWithAdmin(Admin admin,
                                                                 String topic,
                                                                 int partitionNumber,
                                                                 long timeoutMs,
                                                                 Optional<Integer> oldLeaderOpt,
                                                                 Optional<Integer> newLeaderOpt) throws Exception {
        if (oldLeaderOpt.isPresent() && newLeaderOpt.isPresent()) {
            throw new IllegalArgumentException("Can't define both the old and the new leader");
        }

        GetPartitionLeader getPartitionLeader = (t, p) -> {
            TopicDescription topicDescription = admin.describeTopics(Collections.singletonList(t)).allTopicNames().get().get(t);
            return topicDescription.partitions().stream()
                    .filter(partitionInfo -> partitionInfo.partition() == p)
                    .findFirst()
                    .map(partitionInfo -> partitionInfo.leader().id() == Node.noNode().id() ? null : partitionInfo.leader().id());
        };

        return doWaitUntilLeaderIsElectedOrChanged(getPartitionLeader, topic, partitionNumber, timeoutMs, oldLeaderOpt, newLeaderOpt);
    }

    private static int doWaitUntilLeaderIsElectedOrChanged(GetPartitionLeader getPartitionLeader,
                                                           String topic,
                                                           int partition,
                                                           long timeoutMs,
                                                           Optional<Integer> oldLeaderOpt,
                                                           Optional<Integer> newLeaderOpt) throws Exception {
        long startTime = System.currentTimeMillis();
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        log.trace("Waiting for leader to be elected or changed for partition {}, old leader is {}, new leader is {}",
            topicPartition, oldLeaderOpt, newLeaderOpt);

        Optional<Integer> leader = Optional.empty();
        Optional<Integer> electedOrChangedLeader = Optional.empty();

        while (electedOrChangedLeader.isEmpty() && System.currentTimeMillis() < startTime + timeoutMs) {
            // Check if the leader is elected
            leader = getPartitionLeader.getPartitionLeader(topic, partition);
            if (leader.isPresent()) {
                int leaderPartitionId = leader.get();
                if (newLeaderOpt.isPresent() && newLeaderOpt.get() == leaderPartitionId) {
                    log.trace("Expected new leader {} is elected for partition {}", leaderPartitionId, topicPartition);
                    electedOrChangedLeader = leader;
                } else if (oldLeaderOpt.isPresent() && oldLeaderOpt.get() != leaderPartitionId) {
                    log.trace("Leader for partition {} is changed from {} to {}", topicPartition, oldLeaderOpt.get(), leaderPartitionId);
                    electedOrChangedLeader = leader;
                } else if (newLeaderOpt.isEmpty() && oldLeaderOpt.isEmpty()) {
                    log.trace("Leader {} is elected for partition {}", leaderPartitionId, topicPartition);
                    electedOrChangedLeader = leader;
                } else {
                    log.trace("Current leader for partition {} is {}", topicPartition, leaderPartitionId);
                }
            } else {
                log.trace("Leader for partition {} is not elected yet", topicPartition);
            }
            Thread.sleep(Math.min(timeoutMs, 100L));
        }

        Optional<Integer> finalLeader = leader;
        return electedOrChangedLeader.orElseThrow(() -> {
            String errorMessage;
            errorMessage = newLeaderOpt.map(integer -> "Timing out after " + timeoutMs + " ms since expected new leader " + integer +
                " was not elected for partition " + topicPartition + ", leader is " + finalLeader).orElseGet(() -> oldLeaderOpt.map(integer -> "Timing out after " + timeoutMs + " ms since a new leader that is different from " + integer +
                " was not elected for partition " + topicPartition + ", leader is " + finalLeader).orElseGet(() -> "Timing out after " + timeoutMs + " ms since a leader was not elected for partition " + topicPartition));
            return new AssertionError(errorMessage);
        });
    }

    public static <B extends KafkaBroker> Map<TopicPartition, UpdateMetadataPartitionState> waitForAllPartitionsMetadata(
        List<B> brokers, String topic, int expectedNumPartitions) throws Exception {

        // Wait until all brokers have the expected partition metadata
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker -> {
            if (expectedNumPartitions == 0) {
                return broker.metadataCache().numPartitions(topic).isEmpty();
            } else {
                return OptionConverters.toJava(broker.metadataCache().numPartitions(topic))
                    .equals(Optional.of(expectedNumPartitions));
            }
        }), 60000, "Topic [" + topic + "] metadata not propagated after 60000 ms");

        // Since the metadata is propagated, we should get the same metadata from each server
        Map<TopicPartition, UpdateMetadataPartitionState> partitionMetadataMap = new HashMap<>();
        IntStream.range(0, expectedNumPartitions).forEach(i -> {
            TopicPartition topicPartition = new TopicPartition(topic, i);
            UpdateMetadataPartitionState partitionState = OptionConverters.toJava(brokers.get(0).metadataCache()
                .getPartitionInfo(topic, i)).orElseThrow(() ->
                new IllegalStateException("Cannot get topic: " + topic + ", partition: " + i + " in server metadata cache"));
            partitionMetadataMap.put(topicPartition, partitionState);
        });

        return partitionMetadataMap;
    }

    @FunctionalInterface
    private interface GetPartitionLeader {
        Optional<Integer> getPartitionLeader(String topic, int partition) throws Exception;
    }
}
