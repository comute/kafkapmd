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
package org.apache.kafka.streams.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.ReadOnlyWindowStoreFacade;

import java.util.List;

public class WindowStoreFacade<K, V> extends ReadOnlyWindowStoreFacade<K, V> implements WindowStore<K, V> {

    public WindowStoreFacade(final TimestampedWindowStore<K, V> store) {
        super(store);
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long windowStartTimestamp) {
        inner.put(key, ValueAndTimestamp.make(value, ConsumerRecord.NO_TIMESTAMP), windowStartTimestamp);
    }

    @Override
    public void flush() {
        inner.flush();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public void put(Windowed<K> key, V value) {

    }

    @Override
    public V putIfAbsent(Windowed<K> key, V value) {
        return null;
    }

    @Override
    public void putAll(List<KeyValue<Windowed<K>, V>> entries) {

    }

    @Override
    public V delete(Windowed<K> key) {
        return null;
    }

    @Override
    public V get(Windowed<K> key) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> range(Windowed<K> from, Windowed<K> to) {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return 0;
    }
}