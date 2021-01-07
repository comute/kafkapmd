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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Cluster;

import java.util.Collections;
import java.util.Set;

public interface ClusterMetadata {
    Set<String> topics();
    Integer partitionCountForTopic(String topic);

    static ClusterMetadata fromMetadataFetchResponse(final Cluster cluster) {
        return new ClusterMetadata() {
            @Override
            public Set<String> topics() {
                return Collections.unmodifiableSet(cluster.topics());
            }

            @Override
            public Integer partitionCountForTopic(final String topic) {
                return cluster.partitionCountForTopic(topic);
            }
        };
    }

    static ClusterMetadata fromAdminClient(final Admin adminClient) {
        return null;
    }
}
