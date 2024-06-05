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
package org.apache.kafka.coordinator.group.api.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Optional;
import java.util.Set;

/**
 * Interface representing the subscription metadata for a group member.
 */
@InterfaceStability.Unstable
public interface MemberSubscriptionSpec {
    /**
     * Gets the rack Id if present.
     *
     * @return An Optional containing the rack Id, or an empty Optional if not present.
     */
    Optional<String> rackId();

    /**
     * Gets the set of subscribed topic Ids.
     *
     * @return The set of subscribed topic Ids.
     */
    Set<Uuid> subscribedTopicIds();
}
