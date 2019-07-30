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
package org.apache.kafka.clients.consumer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class PartitionAssignorAdapter implements ConsumerPartitionAssignor {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionAssignorAdapter.class);
    private final PartitionAssignor oldAssignor;

    PartitionAssignorAdapter(PartitionAssignor oldAssignor) {
        this.oldAssignor = oldAssignor;
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return oldAssignor.subscription(topics).userData();
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        return oldToNewGroupAssignment(oldAssignor.assign(metadata, newToOldGroupSubscription(groupSubscription)));
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        oldAssignor.onAssignment(newToOldAssignment(assignment), metadata.generationId());
    }

    @Override
    public String name() {
        return oldAssignor.name();
    }

    private static PartitionAssignor.Assignment newToOldAssignment(Assignment assignment) {
        return new PartitionAssignor.Assignment(assignment.partitions(), assignment.userData());
    }

    private Map<String, PartitionAssignor.Subscription> newToOldGroupSubscription(GroupSubscription subscriptions) {
        Map<String, PartitionAssignor.Subscription> oldSubscriptions = new HashMap<>();
        for (Map.Entry<String, Subscription> entry : subscriptions.groupSubscription().entrySet()) {
            String member = entry.getKey();
            Subscription newSubscription = entry.getValue();
            oldSubscriptions.put(member, new PartitionAssignor.Subscription(
                newSubscription.topics(), newSubscription.userData()));
        }
        return oldSubscriptions;
    }

    private GroupAssignment oldToNewGroupAssignment(Map<String, PartitionAssignor.Assignment> assignments) {
        Map<String, Assignment> newAssignments = new HashMap<>();
        for (Map.Entry<String, PartitionAssignor.Assignment> entry : assignments.entrySet()) {
            String member = entry.getKey();
            PartitionAssignor.Assignment oldAssignment = entry.getValue();
            newAssignments.put(member, new Assignment(oldAssignment.partitions(), oldAssignment.userData()));
        }
        return new GroupAssignment(newAssignments);
    }

    public static List<ConsumerPartitionAssignor> getAssignorInstances(List<String> assignorClasses, Map<String, Object> configs) {
        List<ConsumerPartitionAssignor> assignors = new ArrayList<>();

        if (assignorClasses == null)
            return assignors;

        for (Object klass : assignorClasses) {
            // first try to get the class if passed in as a string
            if (klass instanceof String) {
                try {
                    klass = Class.forName((String) klass, true, Utils.getContextOrKafkaClassLoader());
                } catch (ClassNotFoundException classNotFound) {
                    throw new KafkaException(klass + " ClassNotFoundException exception occurred", classNotFound);
                }
            }

            if (klass instanceof Class<?>) {
                Object assignor = Utils.newInstance((Class<?>) klass);
                if (assignor instanceof Configurable)
                    ((Configurable) assignor).configure(configs);

                if (assignor instanceof ConsumerPartitionAssignor) {
                    assignors.add((ConsumerPartitionAssignor) assignor);
                } else if (assignor instanceof PartitionAssignor) {
                    assignors.add(new PartitionAssignorAdapter((PartitionAssignor) assignor));
                    LOG.warn("The PartitionAssignor interface has been deprecated, "
                        + "please implement the ConsumerPartitionAssignor interface instead.");
                } else {
                    throw new KafkaException(klass + " is not an instance of " + PartitionAssignor.class.getName()
                        + " or an instance of " + ConsumerPartitionAssignor.class.getName());
                }
            } else {
                throw new KafkaException("List contains element of type " + klass.getClass().getName() + ", expected String or Class");
            }
        }
        return assignors;
    }
}
