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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineLong;

import java.util.Objects;
import java.util.Optional;

/**
 * Util class to track the offsets written into the internal topic
 * per share partition key.
 * It calculates the minimum offset globally up to which the records
 * in the internal partition are redundant i.e. they have been overridden
 * by newer records.
 */
public class ShareCoordinatorOffsetsManager {

    // Map to store share partition key => current partition offset
    // being written.
    private final TimelineHashMap<SharePartitionKey, Long> offsets;

    // Minimum offset representing the smallest necessary offset (non-redundant)
    // across the internal partition.
    // We are using timeline object here because the offsets which are passed into
    // updateState might not be committed yet. In case of retry, these offsets would
    // be invalidated via the snapshot registry. Hence, using timeline hashmaps
    // the values would automatically revert in accordance with the last committed offset.
    private final TimelineLong minOffset;
    private final TimelineLong redundantOffset;

    public ShareCoordinatorOffsetsManager(SnapshotRegistry snapshotRegistry) {
        Objects.requireNonNull(snapshotRegistry);
        offsets = new TimelineHashMap<>(snapshotRegistry, 0);
        minOffset = new TimelineLong(snapshotRegistry);
        minOffset.set(Long.MAX_VALUE);  // For easy min update.
        redundantOffset = new TimelineLong(snapshotRegistry);
    }

    /**
     * Method updates internal state with the supplied offset for the provided
     * share partition key. It then calculates the minimum offset, if possible,
     * below which all offsets are redundant. This value is then returned as
     * an optional.
     * <p>
     * The value returned is exclusive, in that all offsets below it but not including
     * it are redundant.
     *
     * @param key    - represents {@link SharePartitionKey} whose offset needs updating
     * @param offset - represents the latest partition offset for provided key
     */
    public void updateState(SharePartitionKey key, long offset) {
        minOffset.set(Math.min(minOffset.get(), offset));
        offsets.put(key, offset);

        Optional<Long> deleteTillOffset = findRedundantOffset();
        deleteTillOffset.ifPresent(off -> {
            minOffset.set(off);
            redundantOffset.set(off);
        });
    }

    private Optional<Long> findRedundantOffset() {
        if (offsets.isEmpty()) {
            return Optional.empty();
        }

        long soFar = Long.MAX_VALUE;

        for (long offset : offsets.values()) {
            // Get min offset among latest offsets
            // for all share keys in the internal partition.
            soFar = Math.min(soFar, offset);

            // minOffset represents the smallest necessary offset
            // and if soFar equals it, we cannot proceed. This can happen
            // if a share partition key hasn't had records written for a while
            // For example,
            // <p>
            // key1:1
            // key2:2 4 6
            // key3:3 5 7
            // <p>
            // We can see in above that offsets 2, 4, 3, 5 are redundant,
            // but we do not have a contiguous prefix starting at minOffset
            // and we cannot proceed.
            if (soFar == minOffset.get()) {
                return Optional.empty();
            }
        }

        return Optional.of(soFar);
    }

    /**
     * Most recent last redundant offset. This method is to be used
     * when the caller wants to query the value of such offset.
     * @return Optional of type Long representing the offset or empty for invalid offset values
     */
    public Optional<Long> lastRedundantOffset() {
        long value = redundantOffset.get();
        if (value <= 0 || value == Long.MAX_VALUE) {
            return Optional.empty();
        }
        // We don't want to send the same value repeatedly
        // as caller might call delete records again and again
        // reducing efficiency hence, once valid redundant value is
        // returned lets set it to 0 so next time onwards
        // empty Optional is returned.
        redundantOffset.set(0L);
        return Optional.of(value);
    }

    // visible for testing
    TimelineHashMap<SharePartitionKey, Long> curState() {
        return offsets;
    }
}
