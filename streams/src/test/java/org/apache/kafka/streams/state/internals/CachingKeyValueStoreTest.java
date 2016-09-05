/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContextImpl;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.state.internals.MemoryLRUCacheBytesTest.memoryCacheEntrySize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CachingKeyValueStoreTest {

    private CachingKeyValueStore<String, String> store;
    private MockProcessorContext context;
    private InMemoryKeyValueStore<Bytes, byte[]> underlyingStore;
    private MemoryLRUCacheBytes cache;
    private int maxCacheSizeBytes;
    private CacheFlushListenerStub cacheFlushListener;
    private String topic;

    @Before
    public void setUp() throws Exception {
        final String storeName = "store";
        underlyingStore = new InMemoryKeyValueStore<>(storeName);
        cacheFlushListener = new CacheFlushListenerStub();
        store = new CachingKeyValueStore<>(underlyingStore, Serdes.String(), Serdes.String(), cacheFlushListener);
        maxCacheSizeBytes = 100;
        cache = new MemoryLRUCacheBytes(maxCacheSizeBytes);
        context = new MockProcessorContext(null, null, null, null, (RecordCollector) null, cache);
        topic = "topic";
        context.setRecordContext(new ProcessorRecordContextImpl(10, 0, 0, topic, null));
        store.init(context, null);
    }

    @Test
    public void shouldPutGetToFromCache() throws Exception {
        store.put("key", "value");
        store.put("key2", "value2");
        assertEquals("value", store.get("key"));
        assertEquals("value2", store.get("key2"));
        // nothing evicted so underlying store should be empty
        assertEquals(2, cache.size());
        assertEquals(0, underlyingStore.approximateNumEntries());
    }

    @Test
    public void shouldFlushEvictedItemsIntoUnderlyingStore() throws Exception {
        int added = addItemsToCache();
        // should only have one record evicted
        assertEquals(1, underlyingStore.approximateNumEntries());
        // 1 dirty key + entries in store;
        assertEquals(added, store.approximateNumEntries());
        assertNotNull(underlyingStore.get(Bytes.wrap("0".getBytes())));
    }

    @Test
    public void shouldForwardDirtyItemToListenerWhenEvicted() throws Exception {
        int numRecords = addItemsToCache();
        assertEquals(1, cacheFlushListener.forwarded.size());
        assertFalse(cacheFlushListener.forwarded.containsKey(String.valueOf(numRecords - 1)));
    }

    @Test
    public void shouldForwardDirtyItemsWhenFlushCalled() throws Exception {
        store.put("1", "a");
        store.flush();
        assertEquals("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldForwardOldValuesWhenEnabled() throws Exception {
        store.enableSendingOldValues();
        store.put("1", "a");
        store.flush();
        store.put("1", "b");
        store.flush();
        assertEquals("b", cacheFlushListener.forwarded.get("1").newValue);
        assertEquals("a", cacheFlushListener.forwarded.get("1").oldValue);
    }

    @Test
    public void shouldIterateAllStoredItems() throws Exception {
        int items = addItemsToCache();
        final KeyValueIterator<String, String> all = store.all();
        final List<String> results = new ArrayList<>();
        while (all.hasNext()) {
            results.add(all.next().key);
        }
        assertEquals(items, results.size());
    }

    @Test
    public void shouldIterateOverRange() throws Exception {
        int items = addItemsToCache();
        final KeyValueIterator<String, String> range = store.range(String.valueOf(0), String.valueOf(items));
        final List<String> results = new ArrayList<>();
        while (range.hasNext()) {
            results.add(range.next().key);
        }
        assertEquals(items, results.size());
    }

    private int addItemsToCache() throws IOException {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < maxCacheSizeBytes) {
            final String kv = String.valueOf(i++);
            store.put(kv, kv);
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic);
        }
        return i;
    }

    private static class CacheFlushListenerStub implements CacheFlushListener<String, String> {
        private Map<String, Change<String>> forwarded = new HashMap<>();

        @Override
        public void forward(final String key, final Change<String> value, final RecordContext recordContext, final InternalProcessorContext context) {
            forwarded.put(key, value);
        }
    }
}