/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.util.List;

public class ProcessorContextImpl extends AbstractProcessorContext implements RecordCollector.Supplier {

    private final StreamTask task;
    private final RecordCollector collector;

    ProcessorContextImpl(final TaskId id,
                         final StreamTask task,
                         final StreamsConfig config,
                         final RecordCollector collector,
                         final ProcessorStateManager stateMgr,
                         final StreamsMetrics metrics,
                         final ThreadCache cache) {
        super(id, task.applicationId(), config, metrics, stateMgr, cache);
        this.task = task;
        this.collector = collector;
    }

    public ProcessorStateManager getStateMgr() {
        return (ProcessorStateManager) stateManager;
    }

    @Override
    public RecordCollector recordCollector() {
        return this.collector;
    }

<<<<<<< HEAD
    @Override
    public Serde<?> keySerde() {
        return this.keySerde;
    }

    @Override
    public Serde<?> valueSerde() {
        return this.valSerde;
    }

    @Override
    public File stateDir() {
        return stateMgr.baseDir();
    }

    @Override
    public StreamsMetrics metrics() {
        return metrics;
    }

    /**
     * @throws IllegalStateException if this method is called before {@link #initialized()}
     */
    @Override
    public void register(StateStore store, StateRestoreCallback stateRestoreCallback) {
        if (initialized)
            throw new IllegalStateException("Can only create state stores during initialization.");

        stateMgr.register(store, stateRestoreCallback);
    }

=======
>>>>>>> 1974e1b0e54abe5fdebd8ff3338df864b7ab60f3
    /**
     * @throws TopologyBuilderException if an attempt is made to access this state store from an unknown node
     */
    @Override
    public StateStore getStateStore(final String name) {
        if (currentNode() == null) {
            throw new TopologyBuilderException("Accessing from an unknown node");
        }

        final StateStore global = stateManager.getGlobalStore(name);
        if (global != null) {
            return global;
        }

        if (!currentNode().stateStores.contains(name)) {
            throw new TopologyBuilderException("Processor " + currentNode().name() + " has no access to StateStore " + name);
        }

        return stateManager.getStore(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value) {
        final ProcessorNode previousNode = currentNode();
        try {
            for (ProcessorNode child : (List<ProcessorNode>) currentNode().children()) {
                setCurrentNode(child);
                child.process(key, value);
            }
        } finally {
            setCurrentNode(previousNode);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        final ProcessorNode previousNode = currentNode();
        final ProcessorNode child = (ProcessorNode<K, V>) currentNode().children().get(childIndex);
        setCurrentNode(child);
        try {
            child.process(key, value);
        } finally {
            setCurrentNode(previousNode);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value, final String childName) {
        for (ProcessorNode child : (List<ProcessorNode<K, V>>) currentNode().children()) {
            if (child.name().equals(childName)) {
                ProcessorNode previousNode = currentNode();
                setCurrentNode(child);
                try {
                    child.process(key, value);
                    return;
                } finally {
                    setCurrentNode(previousNode);
                }
            }
        }
    }

    @Override
    public void commit() {
        task.needCommit();
    }

    @Override
    public void schedule(final long interval) {
        task.schedule(interval);
    }

}
