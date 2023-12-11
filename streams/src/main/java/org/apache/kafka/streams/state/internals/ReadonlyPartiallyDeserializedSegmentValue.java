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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.query.ResultOrder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


final class ReadonlyPartiallyDeserializedSegmentValue {

    private static final int TIMESTAMP_SIZE = 8;
    private static final int VALUE_SIZE = 4;
    private byte[] segmentValue;
    private long nextTimestamp;
    private long minTimestamp;

    private int deserIndex = -1; // index up through which this segment has been deserialized (inclusive)

    private Map<Integer, Integer> cumulativeValueSizes;

    private int valuesStartingIndex = -1; // the index of the first value in the segment (but the last one in the list)
    private Map<Integer, TimestampAndValueSize> unpackedTimestampAndValueSizes = new HashMap<>();
    private int recordNumber = -1; // number of segment records


    ReadonlyPartiallyDeserializedSegmentValue(final byte[] segmentValue) {
        this.segmentValue = segmentValue;
        this.nextTimestamp =
                RocksDBVersionedStoreSegmentValueFormatter.getNextTimestamp(segmentValue);
        this.minTimestamp =
                RocksDBVersionedStoreSegmentValueFormatter.getMinTimestamp(segmentValue);
        resetDeserHelpers();
    }


    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getNextTimestamp() {
        return nextTimestamp;
    }

    public byte[] serialize() {
        return segmentValue;
    }


    public RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult find(
            final long fromTime, final long toTime, final ResultOrder order, final int index) {

        // this segment does not have any record in query specified time range
        if (toTime < minTimestamp || fromTime > nextTimestamp) {
            return null;
        }

        final boolean isAscending = order.equals(ResultOrder.ASCENDING);

        if (isAscending && valuesStartingIndex == -1) {
            findValuesStartingIndex();
            deserIndex = recordNumber;
        }

        long currTimestamp = -1;
        long currNextTimestamp = -1;
        int currIndex = initializeCurrentIndex(index, isAscending);
        int cumValueSize = initializeCumvalueSize(index, currIndex, isAscending);
        int currValueSize;


        while (hasStillRecord(currTimestamp, currNextTimestamp, order)) {
            if (hasBeenDeserialized(isAscending, currIndex)) {
                final TimestampAndValueSize curr;
                curr = unpackedTimestampAndValueSizes.get(currIndex);
                currTimestamp = curr.timestamp;
                cumValueSize = cumulativeValueSizes.get(currIndex);
                currValueSize = curr.valueSize;

                // update currValueSize
                if (currValueSize == Integer.MIN_VALUE) {
                    final int timestampSegmentIndex = getTimestampIndex(order, currIndex);
                    currValueSize = ByteBuffer.wrap(segmentValue).getInt(timestampSegmentIndex + TIMESTAMP_SIZE);
                    unpackedTimestampAndValueSizes.put(currIndex, new TimestampAndValueSize(currTimestamp, cumValueSize));
                }

                currNextTimestamp = updateCurrNextTimestamp(currIndex, isAscending);

            } else {
                final int timestampSegmentIndex = getTimestampIndex(order, currIndex);
                currTimestamp = ByteBuffer.wrap(segmentValue).getLong(timestampSegmentIndex);
                currValueSize = ByteBuffer.wrap(segmentValue).getInt(timestampSegmentIndex + TIMESTAMP_SIZE);
                currNextTimestamp = timestampSegmentIndex == 2 * TIMESTAMP_SIZE
                        ? nextTimestamp // if this is the first record metadata (timestamp + value size)
                        : ByteBuffer.wrap(segmentValue).getLong(timestampSegmentIndex - (TIMESTAMP_SIZE + VALUE_SIZE));
                cumValueSize += Math.max(currValueSize, 0);

                // update deserHelpers
                deserIndex = currIndex;
                unpackedTimestampAndValueSizes.put(currIndex, new TimestampAndValueSize(currTimestamp, currValueSize));
                cumulativeValueSizes.put(currIndex, cumValueSize);
            }

            if (currValueSize >= 0) {
                final byte[] value = new byte[currValueSize];
                final int valueSegmentIndex = getValueSegmentIndex(order, cumValueSize, currValueSize);
                System.arraycopy(segmentValue, valueSegmentIndex, value, 0, currValueSize);
                if (currTimestamp <= toTime && currNextTimestamp > fromTime) {
                    return new RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult(currIndex, currTimestamp, currNextTimestamp, value);
                }
            }
            // prep for next iteration
            currIndex = isAscending ? currIndex - 1 : currIndex + 1;
        }
        // search in segment expected to find result but did not
        return null;
    }

    private long updateCurrNextTimestamp(final int currIndex, final boolean isAscending) {
        if (isAscending) {
            return currIndex == recordNumber - 1 ? nextTimestamp : unpackedTimestampAndValueSizes.get(currIndex + 1).timestamp;
        } else {
            return currIndex == 0 ? nextTimestamp : unpackedTimestampAndValueSizes.get(currIndex - 1).timestamp;
        }
    }

    private int initializeCumvalueSize(final int index, final int currIndex, final boolean isAscending) {
        return (index == Integer.MAX_VALUE || (!isAscending && index == 0)) ? 0
                                                                            : isAscending ? cumulativeValueSizes.get(currIndex + 1)
                                                                                          : cumulativeValueSizes.get(currIndex - 1);
    }

    private int initializeCurrentIndex(final int index, final boolean isAscending) {
        return isAscending && index == Integer.MAX_VALUE ? recordNumber - 1 : index;
    }


    private boolean hasStillRecord(final long currTimestamp, final long currNextTimestamp, final ResultOrder order) {
        return order.equals(ResultOrder.ASCENDING) ? currNextTimestamp != nextTimestamp : currTimestamp != minTimestamp;
    }

    private boolean hasBeenDeserialized(final boolean isAscending, final int currIndex) {
        if (!isAscending) {
            return currIndex <= deserIndex;
        }
        return currIndex >= deserIndex;
    }

    private int getValueSegmentIndex(final ResultOrder order, final int currentCumValueSize, final int currValueSize) {
        return order.equals(ResultOrder.ASCENDING) ? valuesStartingIndex + (currentCumValueSize - currValueSize)
                                                   : segmentValue.length - currentCumValueSize;
    }

    private int getTimestampIndex(final ResultOrder order, final int currIndex) {
        return order.equals(ResultOrder.ASCENDING) ? valuesStartingIndex - ((recordNumber - currIndex) * (TIMESTAMP_SIZE + VALUE_SIZE))
                                                   : 2 * TIMESTAMP_SIZE + currIndex * (TIMESTAMP_SIZE + VALUE_SIZE);
    }

    private void findValuesStartingIndex() {
        long currTimestamp = -1;
        int currIndex = 0;
        int timestampSegmentIndex = 0;
        while (currTimestamp != minTimestamp) {
            timestampSegmentIndex = 2 * TIMESTAMP_SIZE + currIndex * (TIMESTAMP_SIZE + VALUE_SIZE);
            currTimestamp = ByteBuffer.wrap(segmentValue).getLong(timestampSegmentIndex);
            unpackedTimestampAndValueSizes.put(currIndex, new TimestampAndValueSize(currTimestamp, Integer.MIN_VALUE));
            currIndex++;
        }
        valuesStartingIndex = timestampSegmentIndex + TIMESTAMP_SIZE + VALUE_SIZE;
        recordNumber = currIndex;
    }

    private void resetDeserHelpers() {
        deserIndex = -1;
        unpackedTimestampAndValueSizes = new HashMap<>();
        cumulativeValueSizes = new HashMap<>();
    }



    private static class TimestampAndValueSize {
        final long timestamp;
        final int valueSize;

        TimestampAndValueSize(final long timestamp, final int valueSize) {
            this.timestamp = timestamp;
            this.valueSize = valueSize;
        }
    }
}
