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

import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractResponse extends AbstractRequestResponse {
    public static final int DEFAULT_THROTTLE_TIME = 0;

    protected Send toSend(String destination, ResponseHeader header, short version) {
        return new NetworkSend(destination, serializeWithHeader(header, version));
    }

    /**
     * Visible for testing, typically {@link #toSend(String, ResponseHeader, short)} should be used instead.
     */
    public ByteBuffer serializeWithHeader(ApiKeys apiKey, short version, int correlationId) {
        return serializeWithHeader(new ResponseHeader(correlationId, apiKey.responseHeaderVersion(version)), version);
    }

    private ByteBuffer serializeWithHeader(ResponseHeader header, short version) {
        Struct headerStruct = header.toStruct();
        Message data = data();
        if (data == null)
            return serialize(headerStruct, toStruct(version));

        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(headerStruct.sizeOf() + data.size(serializationCache, version));
        headerStruct.writeTo(buffer);
        data.write(new ByteBufferAccessor(buffer), serializationCache, version);
        buffer.rewind();
        return buffer;
    }

    public abstract Map<Errors, Integer> errorCounts();

    protected Map<Errors, Integer> errorCounts(Errors error) {
        return Collections.singletonMap(error, 1);
    }

    protected Map<Errors, Integer> errorCounts(Collection<Errors> errors) {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (Errors error : errors)
            updateErrorCounts(errorCounts, error);
        return errorCounts;
    }

    protected Map<Errors, Integer> apiErrorCounts(Map<?, ApiError> errors) {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (ApiError apiError : errors.values())
            updateErrorCounts(errorCounts, apiError.error());
        return errorCounts;
    }

    protected void updateErrorCounts(Map<Errors, Integer> errorCounts, Errors error) {
        Integer count = errorCounts.get(error);
        errorCounts.put(error, count == null ? 1 : count + 1);
    }

    protected abstract Struct toStruct(short version);

    public static AbstractResponse parseResponse(ApiKeys apiKey, ByteBuffer responseBuffer, short version) {
        switch (apiKey) {
            case PRODUCE:
                return ProduceResponse.parse(responseBuffer, version);
            case FETCH:
                return FetchResponse.parse(responseBuffer, version);
            case LIST_OFFSETS:
                return ListOffsetResponse.parse(responseBuffer, version);
            case METADATA:
                return MetadataResponse.parse(responseBuffer, version);
            case OFFSET_COMMIT:
                return OffsetCommitResponse.parse(responseBuffer, version);
            case OFFSET_FETCH:
                return OffsetFetchResponse.parse(responseBuffer, version);
            case FIND_COORDINATOR:
                return FindCoordinatorResponse.parse(responseBuffer, version);
            case JOIN_GROUP:
                return JoinGroupResponse.parse(responseBuffer, version);
            case HEARTBEAT:
                return HeartbeatResponse.parse(responseBuffer, version);
            case LEAVE_GROUP:
                return LeaveGroupResponse.parse(responseBuffer, version);
            case SYNC_GROUP:
                return SyncGroupResponse.parse(responseBuffer, version);
            case STOP_REPLICA:
                return StopReplicaResponse.parse(responseBuffer, version);
            case CONTROLLED_SHUTDOWN:
                return ControlledShutdownResponse.parse(responseBuffer, version);
            case UPDATE_METADATA:
                return UpdateMetadataResponse.parse(responseBuffer, version);
            case LEADER_AND_ISR:
                return LeaderAndIsrResponse.parse(responseBuffer, version);
            case DESCRIBE_GROUPS:
                return DescribeGroupsResponse.parse(responseBuffer, version);
            case LIST_GROUPS:
                return ListGroupsResponse.parse(responseBuffer, version);
            case SASL_HANDSHAKE:
                return SaslHandshakeResponse.parse(responseBuffer, version);
            case API_VERSIONS:
                return ApiVersionsResponse.parse(responseBuffer, version);
            case CREATE_TOPICS:
                return CreateTopicsResponse.parse(responseBuffer, version);
            case DELETE_TOPICS:
                return DeleteTopicsResponse.parse(responseBuffer, version);
            case DELETE_RECORDS:
                return DeleteRecordsResponse.parse(responseBuffer, version);
            case INIT_PRODUCER_ID:
                return InitProducerIdResponse.parse(responseBuffer, version);
            case OFFSET_FOR_LEADER_EPOCH:
                return OffsetsForLeaderEpochResponse.parse(responseBuffer, version);
            case ADD_PARTITIONS_TO_TXN:
                return AddPartitionsToTxnResponse.parse(responseBuffer, version);
            case ADD_OFFSETS_TO_TXN:
                return AddOffsetsToTxnResponse.parse(responseBuffer, version);
            case END_TXN:
                return EndTxnResponse.parse(responseBuffer, version);
            case WRITE_TXN_MARKERS:
                return WriteTxnMarkersResponse.parse(responseBuffer, version);
            case TXN_OFFSET_COMMIT:
                return TxnOffsetCommitResponse.parse(responseBuffer, version);
            case DESCRIBE_ACLS:
                return DescribeAclsResponse.parse(responseBuffer, version);
            case CREATE_ACLS:
                return CreateAclsResponse.parse(responseBuffer, version);
            case DELETE_ACLS:
                return DeleteAclsResponse.parse(responseBuffer, version);
            case DESCRIBE_CONFIGS:
                return DescribeConfigsResponse.parse(responseBuffer, version);
            case ALTER_CONFIGS:
                return AlterConfigsResponse.parse(responseBuffer, version);
            case ALTER_REPLICA_LOG_DIRS:
                return AlterReplicaLogDirsResponse.parse(responseBuffer, version);
            case DESCRIBE_LOG_DIRS:
                return DescribeLogDirsResponse.parse(responseBuffer, version);
            case SASL_AUTHENTICATE:
                return SaslAuthenticateResponse.parse(responseBuffer, version);
            case CREATE_PARTITIONS:
                return CreatePartitionsResponse.parse(responseBuffer, version);
            case CREATE_DELEGATION_TOKEN:
                return CreateDelegationTokenResponse.parse(responseBuffer, version);
            case RENEW_DELEGATION_TOKEN:
                return RenewDelegationTokenResponse.parse(responseBuffer, version);
            case EXPIRE_DELEGATION_TOKEN:
                return ExpireDelegationTokenResponse.parse(responseBuffer, version);
            case DESCRIBE_DELEGATION_TOKEN:
                return DescribeDelegationTokenResponse.parse(responseBuffer, version);
            case DELETE_GROUPS:
                return DeleteGroupsResponse.parse(responseBuffer, version);
            case ELECT_LEADERS:
                return ElectLeadersResponse.parse(responseBuffer, version);
            case INCREMENTAL_ALTER_CONFIGS:
                return IncrementalAlterConfigsResponse.parse(responseBuffer, version);
            case ALTER_PARTITION_REASSIGNMENTS:
                return AlterPartitionReassignmentsResponse.parse(responseBuffer, version);
            case LIST_PARTITION_REASSIGNMENTS:
                return ListPartitionReassignmentsResponse.parse(responseBuffer, version);
            case OFFSET_DELETE:
                return OffsetDeleteResponse.parse(responseBuffer, version);
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `parseResponse`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }

    /**
     * Returns whether or not client should throttle upon receiving a response of the specified version with a non-zero
     * throttle time. Client-side throttling is needed when communicating with a newer version of broker which, on
     * quota violation, sends out responses before throttling.
     */
    public boolean shouldClientThrottle(short version) {
        return false;
    }

    public abstract int throttleTimeMs();

    public String toString(short version) {
        return toStruct(version).toString();
    }
}
