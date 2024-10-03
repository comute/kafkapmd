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
package org.apache.kafka.jmh.record;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class GzipCompressedRecordBatchValidationBenchmark extends AbstractCompressedRecordBatchValidationBenchmark {
    @Param(value = {"1", "5", "9"})
    private int level = CompressionType.GZIP_DEFAULT_LEVEL;

    @Param(value = {"512", "8192", "32768"})
    private int bufferSize = CompressionType.GZIP_DEFAULT_BUFFER;

    @Override
    Compression compression() {
        return Compression.gzip().level(level).bufferSize(bufferSize).build();
    }
}
