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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class WriteTxnMarkerResponse extends AbstractResponse {
    private static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    // Possible error codes:
    //   CorruptRecord
    //   InvalidProducerEpoch
    //   UnknownTopicOrPartition
    //   NotLeaderForPartition
    //   MessageTooLarge
    //   RecordListTooLarge
    //   NotEnoughReplicas
    //   NotEnoughReplicasAfterAppend
    //   InvalidRequiredAcks

    private final Map<TopicPartition, Errors> errors;

    public WriteTxnMarkerResponse(Map<TopicPartition, Errors> errors) {
        this.errors = errors;
    }

    public WriteTxnMarkerResponse(Struct struct) {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        Object[] topicPartitionsArray = struct.getArray(TOPIC_PARTITIONS_KEY_NAME);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionObj;
                Integer partition = partitionStruct.getInt(PARTITION_KEY_NAME);
                Errors error = Errors.forCode(partitionStruct.getShort(ERROR_CODE_KEY_NAME));
                errors.put(new TopicPartition(topic, partition), error);
            }
        }
        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.WRITE_TXN_MARKER.responseSchema(version));
        Map<String, Map<Integer, Errors>> mappedPartitions = CollectionUtils.groupDataByTopic(errors);
        Object[] partitionsArray = new Object[mappedPartitions.size()];
        int i = 0;
        for (Map.Entry<String, Map<Integer, Errors>> topicAndPartitions : mappedPartitions.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPIC_PARTITIONS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_KEY_NAME, topicAndPartitions.getKey());
            Map<Integer, Errors> partitionAndErrors = topicAndPartitions.getValue();

            Object[] partitionAndErrorsArray = new Object[partitionAndErrors.size()];
            int j = 0;
            for (Map.Entry<Integer, Errors> partitionAndError : partitionAndErrors.entrySet()) {
                Struct partitionAndErrorStruct = topicPartitionsStruct.instance(PARTITIONS_KEY_NAME);
                partitionAndErrorStruct.set(PARTITION_KEY_NAME, partitionAndError.getKey());
                partitionAndErrorStruct.set(ERROR_CODE_KEY_NAME, partitionAndError.getValue().code());
                partitionAndErrorsArray[j++] = partitionAndErrorStruct;
            }
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, partitionAndErrorsArray);
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPIC_PARTITIONS_KEY_NAME, partitionsArray);
        return struct;
    }

    public Map<TopicPartition, Errors> errors() {
        return errors;
    }

    public static WriteTxnMarkerResponse parse(ByteBuffer buffer, short version) {
        return new WriteTxnMarkerResponse(ApiKeys.WRITE_TXN_MARKER.parseResponse(version, buffer));
    }

}
