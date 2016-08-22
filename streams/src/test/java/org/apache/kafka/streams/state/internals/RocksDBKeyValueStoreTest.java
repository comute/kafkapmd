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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;
import org.rocksdb.Options;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class RocksDBKeyValueStoreTest extends AbstractKeyValueStoreTest {

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(
            ProcessorContext context,
            Class<K> keyClass,
            Class<V> valueClass,
            boolean useContextSerdes) {

        StateStoreSupplier supplier;
        if (useContextSerdes) {
            supplier = Stores.create("my-store").withKeys(context.keySerde()).withValues(context.valueSerde()).persistent().build();
        } else {
            supplier = Stores.create("my-store").withKeys(keyClass).withValues(valueClass).persistent().build();
        }

        ((ForwardingSupplier) supplier).withFlushListener(new CacheFlushListener() {
            @Override
            public void flushed(final Object key, final Change value, final RecordContext recordContext) {

            }
        });

        KeyValueStore<K, V> store = (KeyValueStore<K, V>) supplier.get();
        store.init(context, store);
        return store;

    }

    public static class TheRocksDbConfigSetter implements RocksDBConfigSetter {

        static boolean called = false;

        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            called = true;
        }
    }

    @Test
    public void shouldUseCustomRocksDbConfigSetter() throws Exception {
        final KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        driver.setConfig(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, TheRocksDbConfigSetter.class);
        createKeyValueStore(driver.context(), Integer.class, String.class, false);
        assertTrue(TheRocksDbConfigSetter.called);
    }

}
