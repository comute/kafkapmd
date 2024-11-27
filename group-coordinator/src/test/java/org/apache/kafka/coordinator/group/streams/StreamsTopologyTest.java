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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamsTopologyTest {

    @Test
    public void streamsTopologyEpochShouldBeCorrect() {
        StreamsTopology topology = new StreamsTopology(1, Collections.emptyMap());
        assertEquals(1, topology.topologyEpoch());
    }

    @Test
    public void subtopologiesShouldBeCorrect() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry("subtopology-1", new Subtopology().setSubtopologyId("subtopology-1")),
            mkEntry("subtopology-2", new Subtopology().setSubtopologyId("subtopology-2"))
        );
        StreamsTopology topology = new StreamsTopology(1, subtopologies);
        assertEquals(subtopologies, topology.subtopologies());
    }

    @Test
    public void requiredTopicsShouldBeCorrect() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry("subtopology-1", new Subtopology()
                .setSourceTopics(Arrays.asList("source-topic-1", "source-topic-2"))
                .setRepartitionSourceTopics(Arrays.asList(
                    new TopicInfo().setName("repartition-topic-1"),
                    new TopicInfo().setName("repartition-topic-2")
                ))
                .setStateChangelogTopics(Arrays.asList(
                    new TopicInfo().setName("changelog-topic-1"),
                    new TopicInfo().setName("changelog-topic-2")
                ))
            ),
            mkEntry("subtopology-2", new Subtopology()
                .setSourceTopics(Arrays.asList("source-topic-3", "source-topic-4"))
                .setRepartitionSourceTopics(Arrays.asList(
                    new TopicInfo().setName("repartition-topic-3"),
                    new TopicInfo().setName("repartition-topic-4")
                ))
            )
        );
        StreamsTopology topology = new StreamsTopology(1, subtopologies);
        Set<String> expectedTopics = new HashSet<>(Arrays.asList(
            "source-topic-1", "source-topic-2", "repartition-topic-1", "repartition-topic-2",
            "source-topic-3", "source-topic-4", "repartition-topic-3", "repartition-topic-4",
            "changelog-topic-1", "changelog-topic-2"
        ));
        assertEquals(expectedTopics, topology.requiredTopics());
    }

    @Test
    public void fromRecordShouldCreateCorrectTopology() {
        StreamsGroupTopologyValue record = new StreamsGroupTopologyValue()
            .setEpoch(1)
            .setSubtopologies(Arrays.asList(
                new Subtopology().setSubtopologyId("subtopology-1"),
                new Subtopology().setSubtopologyId("subtopology-2")
            ));
        StreamsTopology topology = StreamsTopology.fromRecord(record);
        assertEquals(1, topology.topologyEpoch());
        assertEquals(2, topology.subtopologies().size());
        assertTrue(topology.subtopologies().containsKey("subtopology-1"));
        assertTrue(topology.subtopologies().containsKey("subtopology-2"));
    }

    @Test
    public void equalsShouldReturnTrueForEqualTopologies() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry("subtopology-1", new Subtopology().setSubtopologyId("subtopology-1")),
            mkEntry("subtopology-2", new Subtopology().setSubtopologyId("subtopology-2"))
        );
        StreamsTopology topology1 = new StreamsTopology(1, subtopologies);
        StreamsTopology topology2 = new StreamsTopology(1, subtopologies);
        assertEquals(topology1, topology2);
    }

    @Test
    public void equalsShouldReturnFalseForDifferentTopologies() {
        Map<String, Subtopology> subtopologies1 = mkMap(
            mkEntry("subtopology-1", new Subtopology().setSubtopologyId("subtopology-1"))
        );
        Map<String, Subtopology> subtopologies2 = mkMap(
            mkEntry("subtopology-2", new Subtopology().setSubtopologyId("subtopology-2"))
        );
        StreamsTopology topology1 = new StreamsTopology(3, subtopologies1);
        StreamsTopology topology2 = new StreamsTopology(4, subtopologies2);
        assertNotEquals(topology1, topology2);
    }

    @Test
    public void hashCodeShouldBeConsistentWithEquals() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry("subtopology-1", new Subtopology().setSubtopologyId("subtopology-1")),
            mkEntry("subtopology-2", new Subtopology().setSubtopologyId("subtopology-2"))
        );
        StreamsTopology topology1 = new StreamsTopology(1, subtopologies);
        StreamsTopology topology2 = new StreamsTopology(1, subtopologies);
        assertEquals(topology1.hashCode(), topology2.hashCode());
    }

    @Test
    public void toStringShouldReturnCorrectRepresentation() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry("subtopology-1", new Subtopology().setSubtopologyId("subtopology-1")),
            mkEntry("subtopology-2", new Subtopology().setSubtopologyId("subtopology-2"))
        );
        StreamsTopology topology = new StreamsTopology(1, subtopologies);
        String expectedString = "StreamsTopology{topologyEpoch=1, subtopologies=" + subtopologies + "}";
        assertEquals(expectedString, topology.toString());
    }
}
