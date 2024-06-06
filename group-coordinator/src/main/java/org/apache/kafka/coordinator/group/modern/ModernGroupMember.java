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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract member common for group members.
 */
public abstract class ModernGroupMember {

    /**
     * The member id.
     */
    protected String memberId;

    /**
     * The current member epoch.
     */
    protected int memberEpoch;

    /**
     * The previous member epoch.
     */
    protected int previousMemberEpoch;

    /**
     * The member state.
     */
    protected MemberState state;

    /**
     * The instance id provided by the member.
     */
    protected String instanceId;

    /**
     * The rack id provided by the member.
     */
    protected String rackId;

    /**
     * The rebalance timeout provided by the member.
     */
    protected int rebalanceTimeoutMs;

    /**
     * The client id reported by the member.
     */
    protected String clientId;

    /**
     * The host reported by the member.
     */
    protected String clientHost;

    /**
     * The list of subscriptions (topic names) configured by the member.
     */
    protected Set<String> subscribedTopicNames;

    /**
     * The partitions assigned to this member.
     */
    protected Map<Uuid, Set<Integer>> assignedPartitions;

    protected ModernGroupMember(
        String memberId,
        int memberEpoch,
        int previousMemberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        Set<String> subscribedTopicNames,
        MemberState state,
        Map<Uuid, Set<Integer>> assignedPartitions
    ) {
        this.memberId = memberId;
        this.memberEpoch = memberEpoch;
        this.previousMemberEpoch = previousMemberEpoch;
        this.state = state;
        this.instanceId = instanceId;
        this.rackId = rackId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.subscribedTopicNames = subscribedTopicNames;
        this.assignedPartitions = assignedPartitions;
    }

    /**
     * @return The member id.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * @return The current member epoch.
     */
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * @return The previous member epoch.
     */
    public int previousMemberEpoch() {
        return previousMemberEpoch;
    }

    /**
     * @return The instance id.
     */
    public String instanceId() {
        return instanceId;
    }

    /**
     * @return The rack id.
     */
    public String rackId() {
        return rackId;
    }

    /**
     * @return The rebalance timeout in millis.
     */
    public int rebalanceTimeoutMs() {
        return rebalanceTimeoutMs;
    }

    /**
     * @return The client id.
     */
    public String clientId() {
        return clientId;
    }

    /**
     * @return The client host.
     */
    public String clientHost() {
        return clientHost;
    }

    /**
     * @return The list of subscribed topic names.
     */
    public Set<String> subscribedTopicNames() {
        return subscribedTopicNames;
    }

    /**
     * @return The current state.
     */
    public MemberState state() {
        return state;
    }

    /**
     * @return True if the member is in the Stable state and at the desired epoch.
     */
    public boolean isReconciledTo(int targetAssignmentEpoch) {
        return state == MemberState.STABLE && memberEpoch == targetAssignmentEpoch;
    }

    /**
     * @return The set of assigned partitions.
     */
    public Map<Uuid, Set<Integer>> assignedPartitions() {
        return assignedPartitions;
    }

    /**
     * @return A boolean indicating whether the member uses the classic protocol, default is false.
     */
    public boolean useClassicProtocol() {
        return false;
    }

    /**
     * @return True of the two provided members have different assigned partitions.
     */
    public static boolean hasAssignedPartitionsChanged(
        ModernGroupMember member1,
        ModernGroupMember member2
    ) {
        return !member1.assignedPartitions().equals(member2.assignedPartitions());
    }

    public static Map<Uuid, Set<Integer>> assignmentFromTopicPartitions(
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> topicPartitionsList
    ) {
        return topicPartitionsList.stream().collect(Collectors.toMap(
            ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions::topicId,
            topicPartitions -> Collections.unmodifiableSet(new HashSet<>(topicPartitions.partitions()))));
    }
}
