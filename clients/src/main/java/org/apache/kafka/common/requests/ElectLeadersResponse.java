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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class ElectLeadersResponse extends AbstractResponse {

    private final ElectLeadersResponseData data;

    public ElectLeadersResponse(ElectLeadersResponseData data) {
        this.data = data;
    }

    public ElectLeadersResponse(
            int throttleTimeMs,
            short errorCode,
            List<ReplicaElectionResult> electionResults,
            short version) {
        this.data = new ElectLeadersResponseData();
        data.setThrottleTimeMs(throttleTimeMs);
        if (version >= 1)
            data.setErrorCode(errorCode);
        data.setReplicaElectionResults(electionResults);
    }

    public ElectLeadersResponseData data() {
        return data;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        for (ReplicaElectionResult result : data.replicaElectionResults()) {
            for (PartitionResult partitionResult : result.partitionResult()) {
                Errors error = Errors.forCode(partitionResult.errorCode());
                counts.put(error, counts.getOrDefault(error, 0) + 1);
            }
        }
        return counts;
    }

    public static ElectLeadersResponse parse(ByteBuffer buffer, short version) {
        return new ElectLeadersResponse(new ElectLeadersResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    public static Map<TopicPartition, Optional<Throwable>> electLeadersResult(ElectLeadersResponseData data) {
        Map<TopicPartition, Optional<Throwable>> map = new HashMap<>();

        for (ElectLeadersResponseData.ReplicaElectionResult topicResults : data.replicaElectionResults()) {
            for (ElectLeadersResponseData.PartitionResult partitionResult : topicResults.partitionResult()) {
                Optional<Throwable> value = Optional.empty();
                Errors error = Errors.forCode(partitionResult.errorCode());
                if (error != Errors.NONE) {
                    value = Optional.of(error.exception(partitionResult.errorMessage()));
                }

                map.put(new TopicPartition(topicResults.topic(), partitionResult.partitionId()),
                        value);
            }
        }

        return map;
    }
}
