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
package org.apache.kafka.storage.log;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.record.FileRecords;

import java.util.Optional;

public class OffsetResultHolder {

    private Optional<FileRecords.TimestampAndOffset> timestampAndOffsetOpt;
    private Optional<AsyncOffsetReadFutureHolder<Either<Exception, FileRecords.TimestampAndOffset>>> futureHolderOpt;
    private Optional<ApiException> maybeOffsetsError = Optional.empty();
    private Optional<Long> lastFetchableOffset = Optional.empty();

    public OffsetResultHolder() {
        this(Optional.empty(), Optional.empty());
    }

    public OffsetResultHolder(
            Optional<FileRecords.TimestampAndOffset> timestampAndOffsetOpt,
            Optional<AsyncOffsetReadFutureHolder<Either<Exception, FileRecords.TimestampAndOffset>>> futureHolderOpt
    ) {
        this.timestampAndOffsetOpt = timestampAndOffsetOpt;
        this.futureHolderOpt = futureHolderOpt;
    }

    public OffsetResultHolder(Optional<FileRecords.TimestampAndOffset> timestampAndOffsetOpt) {
        this(timestampAndOffsetOpt, Optional.empty());
    }

    public Optional<FileRecords.TimestampAndOffset> timestampAndOffsetOpt() {
        return timestampAndOffsetOpt;
    }

    public Optional<AsyncOffsetReadFutureHolder<Either<Exception, FileRecords.TimestampAndOffset>>> futureHolderOpt() {
        return futureHolderOpt;
    }

    public Optional<ApiException> maybeOffsetsError() {
        return maybeOffsetsError;
    }

    public Optional<Long> lastFetchableOffset() {
        return lastFetchableOffset;
    }

    public void timestampAndOffsetOpt(Optional<FileRecords.TimestampAndOffset> timestampAndOffsetOpt) {
        this.timestampAndOffsetOpt = timestampAndOffsetOpt;
    }

    public void futureHolderOpt(Optional<AsyncOffsetReadFutureHolder<Either<Exception, FileRecords.TimestampAndOffset>>> futureHolderOpt) {
        this.futureHolderOpt = futureHolderOpt;
    }

    public void maybeOffsetsError(Optional<ApiException> maybeOffsetsError) {
        this.maybeOffsetsError = maybeOffsetsError;
    }

    public void lastFetchableOffset(Optional<Long> lastFetchableOffset) {
        this.lastFetchableOffset = lastFetchableOffset;
    }
}
