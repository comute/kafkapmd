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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorNode<K, V> {

    // TODO: 'children' can be removed when #forward() via index is removed
    private final List<ProcessorNode<?, ?>> children;
    private final Map<String, ProcessorNode<?, ?>> childByName;

    protected final Processor<K, V> processor;
    private NodeMetrics nodeMetrics;
    private final String name;
    private final Time time;

    private K key;
    private V value;
    private final Runnable processDelegate = new Runnable() {
        @Override
        public void run() {
            processor.process(key, value);
        }
    };
    private ProcessorContext context;
    private final Runnable initDelegate = new Runnable() {
        @Override
        public void run() {
            if (processor != null) {
                processor.init(context);
            }
        }
    };
    private final Runnable closeDelegate = new Runnable() {
        @Override
        public void run() {
            if (processor != null) {
                processor.close();
            }
        }
    };

    public final Set<String> stateStores;

    public ProcessorNode(final String name) {
        this(name, null, null);
    }


    public ProcessorNode(final String name, final Processor<K, V> processor, final Set<String> stateStores) {
        this.name = name;
        this.processor = processor;
        this.children = new ArrayList<>();
        this.childByName = new HashMap<>();
        this.stateStores = stateStores;
        this.time = new SystemTime();
    }


    public final String name() {
        return name;
    }

    public final Processor<K, V> processor() {
        return processor;
    }

    public final List<ProcessorNode<?, ?>> children() {
        return children;
    }

    final ProcessorNode getChild(final String childName) {
        return childByName.get(childName);
    }

    public void addChild(final ProcessorNode<?, ?> child) {
        children.add(child);
        childByName.put(child.name, child);
    }

    public void init(final ProcessorContext context) {
        this.context = context;
        try {
            nodeMetrics = new NodeMetrics(context.metrics(), name, context);
            nodeMetrics.metrics.measureLatencyNs(time, initDelegate, nodeMetrics.nodeCreationSensor);
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to initialize processor %s", name), e);
        }
    }

    public void close() {
        try {
            nodeMetrics.metrics.measureLatencyNs(time, closeDelegate, nodeMetrics.nodeDestructionSensor);
            nodeMetrics.removeAllSensors();
        } catch (final Exception e) {
            throw new StreamsException(String.format("failed to close processor %s", name), e);
        }
    }


    public void process(final K key, final V value) {
        this.key = key;
        this.value = value;

        this.nodeMetrics.metrics.measureLatencyNs(time, processDelegate, nodeMetrics.nodeProcessTimeSensor);
    }

    public void punctuate(final long timestamp, final Punctuator punctuator) {
        final Runnable punctuateDelegate = new Runnable() {
            @Override
            public void run() {
                punctuator.punctuate(timestamp);
            }
        };
        this.nodeMetrics.metrics.measureLatencyNs(time, punctuateDelegate, nodeMetrics.nodePunctuateTimeSensor);
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * @return a string representation of this node starting with the given indent, useful for debugging.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder(indent + name + ":\n");
        if (stateStores != null && !stateStores.isEmpty()) {
            sb.append(indent).append("\tstates:\t\t[");
            for (final String store : stateStores) {
                sb.append(store);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);  // remove the last comma
            sb.append("]\n");
        }
        return sb.toString();
    }

    Sensor sourceNodeForwardSensor() {
        return nodeMetrics.sourceNodeForwardSensor;
    }

    Sensor sourceNodeSkippedDueToDeserializationErrorSensor() {
        return nodeMetrics.sourceNodeSkippedDueToDeserializationError;
    }

    private static final class NodeMetrics {
        private final StreamsMetricsImpl metrics;

        private final Sensor nodeProcessTimeSensor;
        private final Sensor nodePunctuateTimeSensor;
        private final Sensor sourceNodeForwardSensor;
        private final Sensor sourceNodeSkippedDueToDeserializationError;
        private final Sensor nodeCreationSensor;
        private final Sensor nodeDestructionSensor;

        private NodeMetrics(final StreamsMetrics metrics, final String name, final ProcessorContext context) {
            final String scope = "processor-node";
            final String tagKey = "task-id";
            final String tagValue = context.taskId().toString();
            this.metrics = (StreamsMetricsImpl) metrics;

            // these are all latency metrics
            this.nodeProcessTimeSensor = metrics.addLatencyAndThroughputSensor(
                scope, name, "process", Sensor.RecordingLevel.DEBUG, tagKey, tagValue
            );
            this.nodePunctuateTimeSensor = metrics.addLatencyAndThroughputSensor(
                scope, name, "punctuate", Sensor.RecordingLevel.DEBUG, tagKey, tagValue
            );
            this.nodeCreationSensor = metrics.addLatencyAndThroughputSensor(
                scope, name, "create", Sensor.RecordingLevel.DEBUG, tagKey, tagValue
            );
            this.nodeDestructionSensor = metrics.addLatencyAndThroughputSensor(
                scope, name, "destroy", Sensor.RecordingLevel.DEBUG, tagKey, tagValue
            );
            this.sourceNodeForwardSensor = metrics.addThroughputSensor(
                scope, name, "forward", Sensor.RecordingLevel.DEBUG, tagKey, tagValue
            );
            this.sourceNodeSkippedDueToDeserializationError = metrics.addThroughputSensor(
                scope, name, "skippedDueToDeserializationError", Sensor.RecordingLevel.DEBUG, tagKey, tagValue
            );
        }

        private void removeAllSensors() {
            metrics.removeSensor(nodeProcessTimeSensor);
            metrics.removeSensor(nodePunctuateTimeSensor);
            metrics.removeSensor(sourceNodeForwardSensor);
            metrics.removeSensor(nodeCreationSensor);
            metrics.removeSensor(nodeDestructionSensor);
            metrics.removeSensor(sourceNodeSkippedDueToDeserializationError);
        }
    }
}
