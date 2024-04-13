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

import java.util.Optional;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTrackerSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.state.internals.LeftOrRightValue;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KStreamKStreamLeftJoin<K, VL, VR, VOut> extends KStreamKStreamJoin<K, VL, VR, VOut, VL, VR> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamLeftJoin.class);

    KStreamKStreamLeftJoin(final String otherWindowName,
            final JoinWindowsInternal windows,
            final ValueJoinerWithKey<? super K, ? super VL, ? super VR, ? extends VOut> joiner,
            final boolean outer,
            final Optional<String> outerJoinWindowName,
            final TimeTrackerSupplier sharedTimeTrackerSupplier) {
        super(otherWindowName, sharedTimeTrackerSupplier, windows.spuriousResultFixEnabled(), outerJoinWindowName,
                windows.beforeMs, windows.afterMs, windows, outer, joiner);
    }

    @Override
    public Processor<K, VL, K, VOut> get() {
        return new KStreamKStreamJoinLeftProcessor();
    }

    private class KStreamKStreamJoinLeftProcessor extends KStreamKStreamJoinProcessor {


        @Override
        public TimestampedKeyAndJoinSide<K> makeThisKey(final K key, final long timestamp) {
            return TimestampedKeyAndJoinSide.makeLeft(key, timestamp);
        }

        @Override
        public LeftOrRightValue<VL, VR> makeThisValue(final VL thisValue) {
            return LeftOrRightValue.makeLeft(thisValue);
        }

        @Override
        public TimestampedKeyAndJoinSide<K> makeOtherKey(final K key, final long timestamp) {
            return TimestampedKeyAndJoinSide.makeRight(key, timestamp);
        }

        @Override
        public VL getThisValue(final LeftOrRightValue<? extends VL, ? extends VR> leftOrRightValue) {
            return leftOrRightValue.getLeftValue();
        }

        @Override
        public VR getOtherValue(final LeftOrRightValue<? extends VL, ? extends VR> leftOrRightValue) {
            return leftOrRightValue.getRightValue();
        }
    }
}
