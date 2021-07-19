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
package org.apache.kafka.clients.admin.internals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class ListConsumerGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> {

    private final boolean requireStable;
    private final Map<String, List<TopicPartition>> groupIdToTopicPartitions;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public ListConsumerGroupOffsetsHandler(
        String groupId,
        List<TopicPartition> partitions,
        LogContext logContext
    ) {
        this(groupId, partitions, false, logContext);
    }

    public ListConsumerGroupOffsetsHandler(
        String groupId,
        List<TopicPartition> partitions,
        boolean requireStable,
        LogContext logContext
    ) {
        this(Collections.singletonMap(groupId, partitions), requireStable, logContext);
    }

    public ListConsumerGroupOffsetsHandler(
        Map<String, List<TopicPartition>> groupIdToTopicPartitions,
        boolean requireStable,
        LogContext logContext
    ) {
        this.log = logContext.logger(ListConsumerGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
        this.groupIdToTopicPartitions = groupIdToTopicPartitions;
        this.requireStable = requireStable;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> newFuture(
        List<String> groupIds
    ) {
        return AdminApiFuture.forKeys(groupIds
            .stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet()));
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> newFuture(
        String groupId
    ) {
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    @Override
    public String apiName() {
        return "offsetFetch";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    private void validateKeys(Set<CoordinatorKey> groupIds) {
        Set<CoordinatorKey> keys = groupIdToTopicPartitions
                .keySet()
                .stream()
                .map(CoordinatorKey::byGroupId)
                .collect(Collectors.toSet());
        if (!keys.containsAll(groupIds)) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                    " (expected one of " + keys + ")");
        }
    }

    @Override
    public OffsetFetchRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> groupIds) {
        validateKeys(groupIds);

        // Create a map that only contains the consumer groups owned by the coordinator.
        Map<String, List<TopicPartition>> coordinatorGroupIdToTopicPartitions = new HashMap<>(groupIds.size());
        groupIds.forEach(g -> coordinatorGroupIdToTopicPartitions.put(g.idValue, groupIdToTopicPartitions.get(g.idValue)));

        // Set the flag to false as for admin client request,
        // we don't need to wait for any pending offset state to clear.
        return new OffsetFetchRequest.Builder(coordinatorGroupIdToTopicPartitions, requireStable, false);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        validateKeys(groupIds);

        final OffsetFetchResponse response = (OffsetFetchResponse) abstractResponse;

        Map<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        List<CoordinatorKey> unmapped = new ArrayList<>();
        for (CoordinatorKey coordinatorKey : groupIds) {
            String group = coordinatorKey.idValue;
            if (response.groupHasError(group)) {
                handleGroupError(CoordinatorKey.byGroupId(group), response.groupLevelError(group), failed, unmapped);
            } else {
                final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = new HashMap<>();
                Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = response.partitionDataMap(group);
                for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> partitionEntry : responseData.entrySet()) {
                    final TopicPartition topicPartition = partitionEntry.getKey();
                    OffsetFetchResponse.PartitionData partitionData = partitionEntry.getValue();
                    final Errors error = partitionData.error;

                    if (error == Errors.NONE) {
                        final long offset = partitionData.offset;
                        final String metadata = partitionData.metadata;
                        final Optional<Integer> leaderEpoch = partitionData.leaderEpoch;
                        // Negative offset indicates that the group has no committed offset for this partition
                        if (offset < 0) {
                            groupOffsetsListing.put(topicPartition, null);
                        } else {
                            groupOffsetsListing.put(topicPartition, new OffsetAndMetadata(offset, leaderEpoch, metadata));
                        }
                    } else {
                        log.warn("Skipping return offset for {} due to error {}.", topicPartition, error);
                    }
                }
                completed.put(CoordinatorKey.byGroupId(group), groupOffsetsListing);
            }
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("`OffsetFetch` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;
            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`OffsetFetch` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`OffsetFetch` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                log.error("`OffsetFetch` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
        }
    }
}
