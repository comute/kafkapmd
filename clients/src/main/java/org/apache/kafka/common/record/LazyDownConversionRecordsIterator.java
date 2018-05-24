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
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.SystemTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation for being able to iterate over down-converted records. Goal of this implementation is to keep
 * it as memory-efficient as possible by not having to maintain all down-converted records in-memory. Maintains
 * a view into batches of down-converted records.
 */
public class LazyDownConversionRecordsIterator extends AbstractIterator<ConvertedRecords> {
    private final AbstractIterator<? extends RecordBatch> batchIterator;
    private final byte toMagic;
    private final long firstOffset;
    private final long maximumReadSize;

    /**
     * @param recordsToDownConvert Records that require down-conversion
     * @param maximumReadSize Maximum possible size of underlying records that will be down-converted in each call to
     *                        {@link #makeNext()}. This limit does not apply for the first message batch to ensure that
     *                        we down-convert at least one batch of messages.
     */
    LazyDownConversionRecordsIterator(Records recordsToDownConvert, byte toMagic, long firstOffset, long maximumReadSize) {
        this.batchIterator = recordsToDownConvert.batchIterator();
        this.toMagic = toMagic;
        this.firstOffset = firstOffset;
        this.maximumReadSize = maximumReadSize;
    }

    /**
     * Make next set of down-converted records
     * @return Down-converted records
     */
    @Override
    protected ConvertedRecords makeNext() {
        List<RecordBatch> batches = new ArrayList<>();
        boolean isFirstBatch = true;
        long sizeSoFar = 0;

        if (!batchIterator.hasNext())
            return allDone();

        // Figure out batches we should down-convert based on the size constraints
        while (batchIterator.hasNext() &&
                (isFirstBatch || (batchIterator.peek().sizeInBytes() + sizeSoFar) <= maximumReadSize)) {
            RecordBatch currentBatch = batchIterator.next();
            batches.add(currentBatch);
            sizeSoFar += currentBatch.sizeInBytes();
            isFirstBatch = false;
        }
        return RecordsUtil.downConvert(batches, toMagic, firstOffset, new SystemTime());
    }
}
