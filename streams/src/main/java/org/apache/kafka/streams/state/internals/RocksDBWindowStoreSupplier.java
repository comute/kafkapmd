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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.processor.StateStore;

/**
 * A {@link org.apache.kafka.streams.state.KeyValueStore} that stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class RocksDBWindowStoreSupplier<K, V> implements ForwardingStateStoreSupplier<Windowed<K>, V> {

    private final String name;
    private final long retentionPeriod;
    private final boolean retainDuplicates;
    private final int numSegments;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Time time;
    private final long windowSize;
    private final boolean enableCaching;
    private CacheFlushListener<Windowed<K>, V> cacheFlushListener = null;

    public RocksDBWindowStoreSupplier(String name, long retentionPeriod, int numSegments, boolean retainDuplicates, Serde<K> keySerde, Serde<V> valueSerde, long windowSize, boolean enableCaching) {
        this(name, retentionPeriod, numSegments, retainDuplicates, keySerde, valueSerde, null, windowSize, enableCaching);
    }

    public RocksDBWindowStoreSupplier(String name, long retentionPeriod, int numSegments, boolean retainDuplicates, Serde<K> keySerde, Serde<V> valueSerde, Time time, long windowSize, boolean enableCaching) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.retainDuplicates = retainDuplicates;
        this.numSegments = numSegments;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
        this.windowSize = windowSize;
        this.enableCaching = enableCaching;
    }

    public String name() {
        return name;
    }

    public StateStore get() {
        RocksDBWindowStore<K, V> store = new RocksDBWindowStore<>(name, retentionPeriod, numSegments, retainDuplicates, keySerde, valueSerde, windowSize, null);
        return new MeteredWindowStore<>(store.enableLogging(), "rocksdb-window", time);
    }

    @Override
    public StateStore get(final CacheFlushListener<Windowed<K>, V> listener) {
        RocksDBWindowStore<K, V> store = new RocksDBWindowStore<>(name, retentionPeriod, numSegments, retainDuplicates, keySerde, valueSerde, windowSize, listener);
        if (enableCaching) {
            store.enableCaching();
        }
        return new MeteredWindowStore<>(store.enableLogging(), "rocksdb-window", time);

    }
}
