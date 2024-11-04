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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A detailed description of a single classic group in the cluster.
 */
public class ClassicGroupDescription {
    private final String groupId;
    private final String protocol;
    private final Collection<MemberDescription> members;
    private final String partitionAssignor;
    private final ClassicGroupState state;
    private final Node coordinator;
    private final Set<AclOperation> authorizedOperations;

    public ClassicGroupDescription(String groupId,
                                   String protocol,
                                   Collection<MemberDescription> members,
                                   String partitionAssignor,
                                   ClassicGroupState state,
                                   Node coordinator) {
        this(groupId, protocol, members, partitionAssignor, state, coordinator, Set.of());
    }

    public ClassicGroupDescription(String groupId,
                                   String protocol,
                                   Collection<MemberDescription> members,
                                   String partitionAssignor,
                                   ClassicGroupState state,
                                   Node coordinator,
                                   Set<AclOperation> authorizedOperations) {
        this.groupId = groupId == null ? "" : groupId;
        this.protocol = protocol;
        this.members = members == null ? List.of() : List.copyOf(members);
        this.partitionAssignor = partitionAssignor == null ? "" : partitionAssignor;
        this.state = state;
        this.coordinator = coordinator;
        this.authorizedOperations = authorizedOperations;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ClassicGroupDescription that = (ClassicGroupDescription) o;
        return Objects.equals(groupId, that.groupId) &&
            Objects.equals(protocol, that.protocol) &&
            Objects.equals(members, that.members) &&
            Objects.equals(partitionAssignor, that.partitionAssignor) &&
            state == that.state &&
            Objects.equals(coordinator, that.coordinator) &&
            Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, protocol, members, partitionAssignor, state, coordinator, authorizedOperations);
    }

    /**
     * The id of the classic group.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * If the group is a simple consumer group or not.
     */
    public boolean isSimpleConsumerGroup() {
        return protocol.isEmpty();
    }

    /**
     * A list of the members of the classic group.
     */
    public Collection<MemberDescription> members() {
        return members;
    }

    /**
     * The classic group partition assignor.
     */
    public String partitionAssignor() {
        return partitionAssignor;
    }

    /**
     * The classic group state, or UNKNOWN if the state is too new for us to parse.
     */
    public ClassicGroupState state() {
        return state;
    }

    /**
     * The classic group coordinator, or null if the coordinator is not known.
     */
    public Node coordinator() {
        return coordinator;
    }

    /**
     * authorizedOperations for this group, or null if that information is not known.
     */
    public  Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId +
            ", protocol='" + protocol + '\'' +
            ", members=" + members.stream().map(MemberDescription::toString).collect(Collectors.joining(",")) +
            ", partitionAssignor=" + partitionAssignor +
            ", state=" + state +
            ", coordinator=" + coordinator +
            ", authorizedOperations=" + authorizedOperations +
            ")";
    }
}
