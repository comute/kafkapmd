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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorCheckpointConfigTest {

    private BackgroundResources backgroundResources;
    @BeforeEach
    public void setUp() {
        backgroundResources = new BackgroundResources();
    }

    @AfterEach
    public void tearDown() {
        backgroundResources.close();
    }

    @Test
    public void testTaskConfigConsumerGroups() {
        List<String> groups = Arrays.asList("consumer-1", "consumer-2", "consumer-3");
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        Map<String, String> props = config.taskConfigForConsumerGroups(groups);
        MirrorCheckpointTaskConfig taskConfig = new MirrorCheckpointTaskConfig(props);
        assertEquals(taskConfig.taskConsumerGroups(), new HashSet<>(groups),
                "Setting consumer groups property configuration failed");
    }

    @Test
    public void testGroupMatching() {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps("groups", "group1"));
        GroupFilter groupFilter = backgroundResources.groupFilter(config, "group filter");
        assertTrue(groupFilter.shouldReplicateGroup("group1"),
                "topic1 group matching property configuration failed");
        assertFalse(groupFilter.shouldReplicateGroup("group2"),
                "topic2 group matching property configuration failed");
    }

    @Test
    public void testNonMutationOfConfigDef() {
        // Sanity check to make sure that these properties are actually defined for the task config,
        // and that the task config class has been loaded and statically initialized by the JVM
        ConfigDef taskConfigDef = MirrorCheckpointTaskConfig.TASK_CONFIG_DEF;
        assertTrue(
                taskConfigDef.names().contains(MirrorCheckpointConfig.TASK_CONSUMER_GROUPS),
                MirrorCheckpointConfig.TASK_CONSUMER_GROUPS + " should be defined for task ConfigDef"
        );

        // Ensure that the task config class hasn't accidentally modified the connector config
        assertFalse(
                MirrorCheckpointConfig.CONNECTOR_CONFIG_DEF.names().contains(MirrorCheckpointConfig.TASK_CONSUMER_GROUPS),
                MirrorCheckpointConfig.TASK_CONSUMER_GROUPS + " should not be defined for connector ConfigDef"
        );
    }

    @Test
    public void testConsumerConfigsForOffsetSyncsTopic() {
        Map<String, String> connectorProps = makeProps(
                "source.consumer.max.partition.fetch.bytes", "1",
                "target.consumer.heartbeat.interval.ms", "1",
                "consumer.max.poll.interval.ms", "1",
                "fetch.min.bytes", "1"
        );
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(connectorProps);
        assertEquals(config.sourceConsumerConfig(), config.offsetSyncsTopicConsumerConfig());
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorCheckpointConfig(connectorProps);
        assertEquals(config.targetConsumerConfig(), config.offsetSyncsTopicConsumerConfig());
    }
}
