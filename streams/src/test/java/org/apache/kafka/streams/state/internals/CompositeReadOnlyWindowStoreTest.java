/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CompositeReadOnlyWindowStoreTest {

    private final String storeName = "window-store";
    private ReadOnlyStoreProviderStub stubProviderOne;
    private ReadOnlyStoreProviderStub stubProviderTwo;
    private CompositeReadOnlyWindowStore<String, String>
        windowStore;
    private ReadOnlyWindowStoreStub<String, String> undelyingWindowStore;
    private ReadOnlyWindowStoreStub<String, String>
        otherUnderylingStore;

    @Before
    public void before() {
        stubProviderOne = new ReadOnlyStoreProviderStub();
        stubProviderTwo = new ReadOnlyStoreProviderStub();
        undelyingWindowStore = new ReadOnlyWindowStoreStub<>();
        stubProviderOne.addWindowStore(storeName, undelyingWindowStore);

        otherUnderylingStore = new ReadOnlyWindowStoreStub<>();
        stubProviderOne.addWindowStore("other-window-store", otherUnderylingStore);

        windowStore = new CompositeReadOnlyWindowStore<>(
            Arrays.<ReadOnlyStoreProvider>asList(stubProviderOne, stubProviderTwo),
            storeName);
    }

    @Test
    public void shouldFetchValuesFromWindowStore() throws Exception {
        undelyingWindowStore.put("my-key", "my-value", 0L);
        undelyingWindowStore.put("my-key", "my-later-value", 10L);

        final WindowStoreIterator<String> iterator = windowStore.fetch("my-key", 0L, 25L);
        final List<KeyValue<Long, String>> results = toList(iterator);

        assertEquals(Arrays.asList(new KeyValue<>(0L, "my-value"),
                                   new KeyValue<>(10L, "my-later-value")),
                     results);
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoData() throws Exception {
        final WindowStoreIterator<String> iterator = windowStore.fetch("my-key", 0L, 25L);
        assertEquals(false, iterator.hasNext());
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() throws Exception {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new
            ReadOnlyWindowStoreStub<>();
        stubProviderTwo.addWindowStore(storeName, secondUnderlying);

        undelyingWindowStore.put("key-one", "value-one", 0L);
        secondUnderlying.put("key-two", "value-two", 10L);

        final List<KeyValue<Long, String>> keyOneResults = toList(windowStore.fetch("key-one", 0L,
                                                                                    1L));
        final List<KeyValue<Long, String>> keyTwoResults = toList(windowStore.fetch("key-two", 10L,
                                                                                    11L));

        assertEquals(Collections.singletonList(KeyValue.pair(0L, "value-one")), keyOneResults);
        assertEquals(Collections.singletonList(KeyValue.pair(10L, "value-two")), keyTwoResults);
    }

    @Test
    public void shouldNotGetValuesFromOtherStores() throws Exception {
        otherUnderylingStore.put("some-key", "some-value", 0L);
        undelyingWindowStore.put("some-key", "my-value", 1L);

        final List<KeyValue<Long, String>> results = toList(windowStore.fetch("some-key", 0L, 2L));
        assertEquals(Collections.singletonList(new KeyValue<>(1L, "my-value")), results);
    }

    private List<KeyValue<Long, String>> toList(final WindowStoreIterator<String> iterator) {
        final List<KeyValue<Long, String>> results = new ArrayList<>();

        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }
}