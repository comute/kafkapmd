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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Arrays;

/**
 * Too much specific information to generalize so the
 * KTable-KTable join requires a specific node.
 */
class KTableKTableJoinNode<K, V1, V2, VR> extends BaseJoinProcessorNode<K, V1, V2, VR> {

    private final String[] joinThisStoreNames;
    private final String[] joinOtherStoreNames;

    KTableKTableJoinNode(final String parentProcessorNodeName,
                         final String processorNodeName,
                         final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner,
                         final JoinProcessorParameters<K, V1> joinThisProcessorParameters,
                         final JoinProcessorParameters<K, V2> joinOtherProcessorParameters,
                         final JoinProcessorParameters<K, VR> joinMergeProcessorParameters,
                         final String thisJoinSide,
                         final String otherJoinSide,
                         final String[] joinThisStoreNames,
                         final String[] joinOtherStoreNames) {

        super(parentProcessorNodeName,
              processorNodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessorParameters,
              joinMergeProcessorParameters,
              thisJoinSide,
              otherJoinSide);

        this.joinThisStoreNames = joinThisStoreNames;
        this.joinOtherStoreNames = joinOtherStoreNames;
    }

    String[] joinThisStoreNames() {
        return Arrays.copyOf(joinThisStoreNames, joinThisStoreNames.length);
    }

    String[] joinOtherStoreNames() {
        return Arrays.copyOf(joinOtherStoreNames, joinOtherStoreNames.length);
    }

    @Override
    void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }

    static <K, V, V1, V2, VR> KTableKTableJoinNodeBuilder<K, V1, V2, VR> kTableKTableJoinNodeBuilder() {
        return new KTableKTableJoinNodeBuilder<>();
    }

    static final class KTableKTableJoinNodeBuilder<K, V1, V2, VR> {

        private String processorNodeName;
        private String parentProcessorNodeName;
        private String[] joinThisStoreNames;
        private JoinProcessorParameters<K, V1> joinThisProcessorParameters;
        private String[] joinOtherStoreNames;
        private JoinProcessorParameters<K, V2> joinOtherProcessorParameters;
        private JoinProcessorParameters<K, VR> joinMergeProcessorParameters;
        private ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner;
        private String thisJoinSide;
        private String otherJoinSide;

        private KTableKTableJoinNodeBuilder() {
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR>  withJoinThisStoreNames(final String[] joinThisStoreNames) {
            this.joinThisStoreNames = joinThisStoreNames;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR>  withJoinThisProcessorParameters(final JoinProcessorParameters<K, V1> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR> withProcessorNodeName(String processorNodeName) {
            this.processorNodeName = processorNodeName;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR>  withJoinOtherStoreNames(final String[] joinOtherStoreNames) {
            this.joinOtherStoreNames = joinOtherStoreNames;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR> withParentProcessorNodeName(final String parentProcessorNodeName) {
            this.parentProcessorNodeName = parentProcessorNodeName;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR>  withJoinOtherProcessorParameters(final JoinProcessorParameters<K, V2> joinOtherProcessorParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessorParameters;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR>  withJoinMergeProcessorParameters(final JoinProcessorParameters<K, VR> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR>  withValueJoiner(final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR>  withThisJoinSide(final String thisJoinSide) {
            this.thisJoinSide = thisJoinSide;
            return this;
        }

        KTableKTableJoinNodeBuilder<K, V1, V2, VR>  withOtherJoinSide(final String otherJoinSide) {
            this.otherJoinSide = otherJoinSide;
            return this;
        }

        KTableKTableJoinNode<K, V1, V2, VR> build() {

            return new KTableKTableJoinNode<>(parentProcessorNodeName,
                                              processorNodeName,
                                              valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              thisJoinSide,
                                              otherJoinSide,
                                              joinThisStoreNames,
                                              joinOtherStoreNames);
        }
    }
}
