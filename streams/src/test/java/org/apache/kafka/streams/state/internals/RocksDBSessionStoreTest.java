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
package org.apache.kafka.streams.state.internals;

import static java.time.Duration.ofMillis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public class RocksDBSessionStoreTest extends AbstractSessionBytesStoreTest {

    private static final String STORE_NAME = "rocksDB session store";

    @Parameter
    public StoreType storeType;

    @Parameter(1)
    public boolean transactional;


    @Parameterized.Parameters(name = "{0} {1}")
    public static Collection<Object[]> data() {
        final List<StoreType> storeTypes = Arrays.asList(StoreType.RocksDBSessionStore,
                                                         StoreType.RocksDBTimeOrderedSessionStoreWithIndex,
                                                         StoreType.RocksDBTimeOrderedSessionStoreWithoutIndex);
        final List<Object[]> data = new ArrayList<>(storeTypes.size() * 2);
        for (final StoreType storeType : storeTypes) {
            data.add(new Object[]{storeType, true});
            data.add(new Object[]{storeType, false});
        }
        return data;
    }

    @Override
    StoreType getStoreType() {
        return storeType;
    }

    @Override
    <K, V> SessionStore<K, V> buildSessionStore(final long retentionPeriod,
                                                 final Serde<K> keySerde,
                                                 final Serde<V> valueSerde) {
        switch (storeType) {
            case RocksDBSessionStore: {
                final SessionBytesStoreSupplier supplier = Stores.persistentSessionStore(
                        STORE_NAME,
                        ofMillis(retentionPeriod),
                        transactional);
                return Stores.sessionStoreBuilder(
                    supplier,
                    keySerde,
                    valueSerde).build();
            }
            case RocksDBTimeOrderedSessionStoreWithIndex: {
                return Stores.sessionStoreBuilder(
                    new RocksDbTimeOrderedSessionBytesStoreSupplier(
                        STORE_NAME,
                        retentionPeriod,
                        true,
                        transactional ? RocksDBTransactionalMechanism.SECONDARY_STORE : null
                    ),
                    keySerde,
                    valueSerde
                ).build();
            }
            case RocksDBTimeOrderedSessionStoreWithoutIndex: {
                return Stores.sessionStoreBuilder(
                    new RocksDbTimeOrderedSessionBytesStoreSupplier(
                        STORE_NAME,
                        retentionPeriod,
                       false,
                        transactional ? RocksDBTransactionalMechanism.SECONDARY_STORE : null
                    ),
                    keySerde,
                    valueSerde
                ).build();
            }
            default:
                throw new IllegalStateException("Unknown StoreType: " + storeType);
        }
    }

}