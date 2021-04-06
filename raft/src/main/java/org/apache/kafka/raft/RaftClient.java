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
package org.apache.kafka.raft;

import org.apache.kafka.snapshot.SnapshotWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

public interface RaftClient<T> extends Closeable {

    interface Listener<T> {
        /**
         * Callback which is invoked for all records committed to the log.
         * It is the responsibility of the caller to invoke {@link BatchReader#close()}
         * after consuming the reader.
         *
         * Note that there is not a one-to-one correspondence between writes through
         * {@link #scheduleAppend(int, List)} or {@link #scheduleAtomicAppend(int, List)}
         * and this callback. The Raft implementation is free to batch together the records
         * from multiple append calls provided that batch boundaries are respected. Records
         * specified through {@link #scheduleAtomicAppend(int, List)} are guaranteed to be a
         * subset of a batch provided by the {@link BatchReader}. Records specified through
         * {@link #scheduleAppend(int, List)} are guaranteed to be in the same order but
         * they can map to any number of batches provided by the {@link BatchReader}.
         *
         * @param reader reader instance which must be iterated and closed
         */
        void handleCommit(BatchReader<T> reader);

        /**
         * Called on any change to leadership. This includes both when a leader is elected and
         * when a leader steps down or fails.
         *
         * @param leader the current leader and epoch
         */
        default void handleLeaderChange(LeaderAndEpoch leader) {}

        default void beginShutdown() {}
    }

    /**
     * Initialize the client.
     * This should only be called once on startup.
     *
     * @throws IOException For any IO errors during initialization
     */
    void initialize() throws IOException;

    /**
     * Register a listener to get commit/leader notifications.
     *
     * @param listener the listener
     */
    void register(Listener<T> listener);

    /**
     * Return the current {@link LeaderAndEpoch}.
     * @return the current {@link LeaderAndEpoch}
     */
    LeaderAndEpoch leaderAndEpoch();

    /**
     * Get local nodeId if one is defined. This may be absent when the client is used
     * as an anonymous observer, as in the case of the metadata shell.
     *
     * @return optional node id
     */
    OptionalInt nodeId();

    /**
     * Append a list of records to the log. The write will be scheduled for some time
     * in the future. There is no guarantee that appended records will be written to
     * the log and eventually committed. While the order of the records is preserve, they can
     * be appended to the log using one or more batches. Each record may be committed independently.
     * If a record is committed, then all records scheduled for append during this epoch
     * and prior to this record are also committed.
     *
     * If the provided current leader epoch does not match the current epoch, which
     * is possible when the state machine has yet to observe the epoch change, then
     * this method will return {@link Long#MAX_VALUE} to indicate an offset which is
     * not possible to become committed. The state machine is expected to discard all
     * uncommitted entries after observing an epoch change.
     *
     * @param epoch the current leader epoch
     * @param records the list of records to append
     * @return the expected offset of the last record; {@link Long#MAX_VALUE} if the records could
     *         be committed; null if no memory could be allocated for the batch at this time
     * @throws RecordBatchTooLargeException if the size of the records is greater than the maximum
     *         batch size; if this exception is throw none of the elements in records were
     *         committed
     */
    Long scheduleAppend(int epoch, List<T> records);

    /**
     * Append a list of records to the log. The write will be scheduled for some time
     * in the future. There is no guarantee that appended records will be written to
     * the log and eventually committed. However, it is guaranteed that if any of the
     * records become committed, then all of them will be.
     *
     * If the provided current leader epoch does not match the current epoch, which
     * is possible when the state machine has yet to observe the epoch change, then
     * this method will return {@link Long#MAX_VALUE} to indicate an offset which is
     * not possible to become committed. The state machine is expected to discard all
     * uncommitted entries after observing an epoch change.
     *
     * @param epoch the current leader epoch
     * @param records the list of records to append
     * @return the expected offset of the last record; {@link Long#MAX_VALUE} if the records could
     *         be committed; null if no memory could be allocated for the batch at this time
     * @throws RecordBatchTooLargeException if the size of the records is greater than the maximum
     *         batch size; if this exception is throw none of the elements in records were
     *         committed
     */
    Long scheduleAtomicAppend(int epoch, List<T> records);

    /**
     * Attempt a graceful shutdown of the client. This allows the leader to proactively
     * resign and help a new leader to get elected rather than forcing the remaining
     * voters to wait for the fetch timeout.
     *
     * Note that if the client has hit an unexpected exception which has left it in an
     * indeterminate state, then the call to shutdown should be skipped. However, it
     * is still expected that {@link #close()} will be used to clean up any resources
     * in use.
     *
     * @param timeoutMs How long to wait for graceful completion of pending operations.
     * @return A future which is completed when shutdown completes successfully or the timeout expires.
     */
    CompletableFuture<Void> shutdown(int timeoutMs);

    /**
     * Resign the leadership. The leader will give up its leadership in the current epoch,
     * and a new election will be held. Note that nothing prevents this leader from getting
     * reelected.
     *
     * @param epoch the epoch to resign from. If this does not match the current epoch, this
     *              call will be ignored.
     */
    void resign(int epoch);

    /**
     * Create a writable snapshot file for a given offset and epoch.
     *
     * The RaftClient assumes that the snapshot return will contain the records up to but
     * not including the end offset in the snapshot id. See {@link SnapshotWriter} for
     * details on how to use this object.
     *
     * @param snapshotId the end offset and epoch that identifies the snapshot
     * @return a writable snapshot
     */
    SnapshotWriter<T> createSnapshot(OffsetAndEpoch snapshotId) throws IOException;
}
