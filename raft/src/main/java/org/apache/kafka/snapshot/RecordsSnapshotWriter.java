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

package org.apache.kafka.snapshot;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.internals.BatchAccumulator.CompletedBatch;
import org.apache.kafka.raft.internals.BatchAccumulator;
//import org.apache.kafka.raft.internals.VoterSet;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.util.Optional;
import java.util.List;
import java.util.OptionalLong;

final public class RecordsSnapshotWriter<T> implements SnapshotWriter<T> {
    final private RawSnapshotWriter snapshot;
    final private BatchAccumulator<T> accumulator;
    final private Time time;

    private RecordsSnapshotWriter(
        RawSnapshotWriter snapshot,
        int maxBatchSize,
        MemoryPool memoryPool,
        Time time,
        CompressionType compressionType,
        RecordSerde<T> serde
    ) {
        this.snapshot = snapshot;
        this.time = time;

        this.accumulator = new BatchAccumulator<>(
            snapshot.snapshotId().epoch(),
            0,
            Integer.MAX_VALUE,
            maxBatchSize,
            memoryPool,
            time,
            compressionType,
            serde
        );
    }

    /**
     * Adds a {@link SnapshotHeaderRecord} to snapshot
     *
     * @throws IllegalStateException if the snapshot is not empty
     */
    private void initializeSnapshotWithHeader(long lastContainedLogTimestamp) {
        if (snapshot.sizeInBytes() != 0) {
            String message = String.format(
                "Initializing writer with a non-empty snapshot: id = '%s'.",
                snapshot.snapshotId()
            );
            throw new IllegalStateException(message);
        }

        SnapshotHeaderRecord headerRecord = new SnapshotHeaderRecord()
            .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
            .setLastContainedLogTimestamp(lastContainedLogTimestamp);
        accumulator.appendSnapshotHeaderRecord(headerRecord, time.milliseconds());
        accumulator.forceDrain();
    }

    /**
     * Adds a {@link SnapshotFooterRecord} to the snapshot
     *
     * No more records should be appended to the snapshot after calling this method
     */
    private void finalizeSnapshotWithFooter() {
        SnapshotFooterRecord footerRecord = new SnapshotFooterRecord()
            .setVersion(ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION);
        accumulator.appendSnapshotFooterRecord(footerRecord, time.milliseconds());
        accumulator.forceDrain();
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshot.snapshotId();
    }

    @Override
    public long lastContainedLogOffset() {
        return snapshot.snapshotId().offset() - 1;
    }

    @Override
    public int lastContainedLogEpoch() {
        return snapshot.snapshotId().epoch();
    }

    @Override
    public boolean isFrozen() {
        return snapshot.isFrozen();
    }

    @Override
    public void append(List<T> records) {
        if (snapshot.isFrozen()) {
            String message = String.format(
                "Append not supported. Snapshot is already frozen: id = '%s'.",
                snapshot.snapshotId()
            );

            throw new IllegalStateException(message);
        }

        accumulator.append(snapshot.snapshotId().epoch(), records, OptionalLong.empty(), false);

        if (accumulator.needsDrain(time.milliseconds())) {
            appendBatches(accumulator.drain());
        }
    }

    @Override
    public long freeze() {
        finalizeSnapshotWithFooter();
        appendBatches(accumulator.drain());
        snapshot.freeze();
        accumulator.close();
        return snapshot.sizeInBytes();
    }

    @Override
    public void close() {
        snapshot.close();
        accumulator.close();
    }

    private void appendBatches(List<CompletedBatch<T>> batches) {
        try {
            for (CompletedBatch<T> batch : batches) {
                snapshot.append(batch.data);
            }
        } finally {
            batches.forEach(CompletedBatch::release);
        }
    }

    final public static class Builder {
        private long lastContainedLogTimestamp = 0;
        private CompressionType compressionType = CompressionType.NONE;
        private Time time = Time.SYSTEM;
        private int maxBatchSize = 1024;
        MemoryPool memoryPool = MemoryPool.NONE;
        // TODO: implement this...
        //private short kraftVersion = 0;
        //private Optional<VoterSet> voterSet = Optional.empty();
        private Optional<RawSnapshotWriter> rawSnapshotWriter = Optional.empty();

        public Builder setLastContainedLogTimestamp(long lastContainedLogTimestamp) {
            this.lastContainedLogTimestamp = lastContainedLogTimestamp;
            return this;
        }

        public Builder setCompressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder setMemoryPool(MemoryPool memoryPool) {
            this.memoryPool = memoryPool;
            return this;
        }

        public Builder setRawSnapshotWriter(RawSnapshotWriter rawSnapshotWriter) {
            this.rawSnapshotWriter = Optional.of(rawSnapshotWriter);
            return this;
        }

        /*
        public Builder setKraftVersion(short kraftVersion) {
            this.kraftVersion = kraftVersion;
            return this;
        }

        public Builder setVoterSet(VoterSet voterSet) {
            this.voterSet = Optional.of(voterSet);
            return this;
        }
        */

        public <T> RecordsSnapshotWriter<T> build(RecordSerde<T> serde) {
            if (!rawSnapshotWriter.isPresent()) {
                throw new IllegalStateException("Builder::build called without a RawSnapshotWriter");
            }

            RecordsSnapshotWriter<T> writer = new RecordsSnapshotWriter<>(
                rawSnapshotWriter.get(),
                maxBatchSize,
                memoryPool,
                time,
                compressionType,
                serde
            );

            // TODO: implement writing kraft version and voter set to the snaopshot
            writer.initializeSnapshotWithHeader(lastContainedLogTimestamp);

            return writer;
        }
    }
}
