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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to represent any type of stateless operation:
 *
 * map, mapValues, flatMap, flatMapValues, filter, filterNot, branch
 *
 */
public class StatelessProcessorNode<K, V> extends StreamsGraphNode {

    private final ProcessorParameters<K, V> processorParameters;

    // some processors need to register multiple parent names with
    // the InternalTopologyBuilder KStream#merge for example.
    // There is only one parent graph node but the name of each KStream merged needs
    // to get registered with InternalStreamsBuilder

    private List<String> multipleParentNames = new ArrayList<>();


    public StatelessProcessorNode(final String nodeName,
                                  final ProcessorParameters processorParameters,
                                  final boolean repartitionRequired) {

        super(nodeName,
              repartitionRequired);

        this.processorParameters = processorParameters;
    }

    public StatelessProcessorNode(final String nodeName,
                           final ProcessorParameters processorParameters,
                           final boolean repartitionRequired,
                           final List<String> multipleParentNames) {

        this(nodeName, processorParameters, repartitionRequired);

        this.multipleParentNames = new ArrayList<>(multipleParentNames);
    }

    ProcessorParameters processorParameters() {
        return processorParameters;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        if (processorParameters != null) {
            if (multipleParentNames.isEmpty()) {
                multipleParentNames.add(parentNode().nodeName());
            }
            topologyBuilder.addProcessor(processorParameters.processorName(), processorParameters.processorSupplier(), multipleParentNames.toArray(new String[]{}));
        }
    }
}
