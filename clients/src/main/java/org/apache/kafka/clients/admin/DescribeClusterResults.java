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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;

/**
 * The results of the describeCluster call.
 */
@InterfaceStability.Unstable
public class DescribeClusterResults {
    private final KafkaFuture<Collection<Node>> nodesFuture;
    private final KafkaFuture<Node> controllerFuture;
    private final KafkaFuture<String> clusterIdFuture;

    DescribeClusterResults(KafkaFuture<Collection<Node>> nodesFuture,
                           KafkaFuture<Node> controllerFuture,
                           KafkaFuture<String> clusterIdFuture) {
        this.nodesFuture = nodesFuture;
        this.controllerFuture = controllerFuture;
        this.clusterIdFuture = clusterIdFuture;
    }

    /**
     * Returns a future which yields a collection of nodes.
     */
    public KafkaFuture<Collection<Node>> nodes() {
        return nodesFuture;
    }

    /**
     * Returns a future which yields the current controller id.
     * Note that this may yield null, if the controller ID is not yet known.
     */
    public KafkaFuture<Node> controller() {
        return controllerFuture;
    }

    /**
     * Returns a future which yields the current cluster Id.
     * Note that this may yield null, if the cluster version is too old.
     */
    public KafkaFuture<String> clusterIdFuture() {
        return clusterIdFuture;
    }
}
