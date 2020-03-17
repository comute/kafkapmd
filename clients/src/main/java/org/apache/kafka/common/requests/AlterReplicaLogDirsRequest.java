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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AlterReplicaLogDirsRequest extends AbstractRequest {

    private final AlterReplicaLogDirsRequestData data;
    private final short version;

    public static class Builder extends AbstractRequest.Builder<AlterReplicaLogDirsRequest> {
        private final AlterReplicaLogDirsRequestData data;

        public Builder(AlterReplicaLogDirsRequestData data) {
            super(ApiKeys.ALTER_REPLICA_LOG_DIRS);
            this.data = data;
        }

        @Override
        public AlterReplicaLogDirsRequest build(short version) {
            return new AlterReplicaLogDirsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public AlterReplicaLogDirsRequest(Struct struct, short version) {
        super(ApiKeys.ALTER_REPLICA_LOG_DIRS, version);
        this.data = new AlterReplicaLogDirsRequestData(struct, version);
        this.version = version;
    }

    public AlterReplicaLogDirsRequest(AlterReplicaLogDirsRequestData data, short version) {
        super(ApiKeys.ALTER_REPLICA_LOG_DIRS, version);
        this.data = data;
        this.version = version;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public AlterReplicaLogDirsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        AlterReplicaLogDirsResponseData data = new AlterReplicaLogDirsResponseData();

        data.setResults(this.data.dirs().stream().flatMap(x -> {
            Stream<AlterReplicaLogDirTopicResult> y = x.topics().stream().map(topic -> {
                return new AlterReplicaLogDirTopicResult()
                        .setTopicName(topic.name())
                        .setPartitions(
                                topic.partitions().stream().map(partitionId -> {
                                    return new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                                            .setErrorCode(Errors.forException(e).code())
                                            .setPartitionIndex(partitionId);
                                }).collect(Collectors.toList()));
            });
            return y;
        }).collect(Collectors.toList()));
        return new AlterReplicaLogDirsResponse(data.setThrottleTimeMs(throttleTimeMs));
    }

    public Map<TopicPartition, String> partitionDirs() {
        Map<TopicPartition, String> result = new HashMap<>();
        for (AlterReplicaLogDirsRequestData.AlterReplicaLogDir alter : data.dirs()) {
            alter.topics().stream().flatMap(t -> {
                Stream<TopicPartition> objectStream = t.partitions().stream().map(
                    p -> new TopicPartition(t.name(), p.intValue()));
                return objectStream;
            }).forEach(tp -> result.put(tp, alter.path()));
        }
        return result;
    }

    public static AlterReplicaLogDirsRequest parse(ByteBuffer buffer, short version) {
        return new AlterReplicaLogDirsRequest(ApiKeys.ALTER_REPLICA_LOG_DIRS.parseRequest(version, buffer), version);
    }
}
