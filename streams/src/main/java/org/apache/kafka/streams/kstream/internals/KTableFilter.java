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

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

class KTableFilter<K, V> implements KTableProcessorSupplier<K, V, V> {

    private final KTableImpl<K, ?, V> parent;
    private final Predicate<? super K, ? super V> predicate;
    private final boolean filterNot;
    private final String storeName;
    private boolean sendOldValues = false;

    public KTableFilter(KTableImpl<K, ?, V> parent, Predicate<? super K, ? super V> predicate,
                        boolean filterNot, String storeName) {
        this.parent = parent;
        this.predicate = predicate;
        this.filterNot = filterNot;
        this.storeName = storeName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return storeName != null ? new MaterializedKTableFilterProcessor() : new KTableFilterProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {

        final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KTableFilterValueGetter(parentValueGetterSupplier.get());
            }

            @Override
            public String[] storeNames() {
                return parentValueGetterSupplier.storeNames();
            }
        };
    }

    @Override
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private V computeValue(K key, V value) {
        V newValue = null;

        if (value != null && (filterNot ^ predicate.test(key, value)))
            newValue = value;

        return newValue;
    }

    private class KTableFilterProcessor extends AbstractProcessor<K, Change<V>> {

        @Override
        public void process(K key, Change<V> change) {
            V newValue = computeValue(key, change.newValue);
            V oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

            if (sendOldValues && oldValue == null && newValue == null)
                return; // unnecessary to forward here.

            context().forward(key, new Change<>(newValue, oldValue));
        }

    }

    private class MaterializedKTableFilterProcessor extends AbstractProcessor<K, Change<V>> {
        private KeyValueStore<K, V> store;
        private TupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<K, V>) context.getStateStore(storeName);
            tupleForwarder = new TupleForwarder<>(store, context, new ForwardingCacheFlushListener<K, V>(context, sendOldValues), sendOldValues);
        }

        @Override
        public void process(K key, Change<V> change) {
            V newValue = computeValue(key, change.newValue);
            V oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

            if (sendOldValues && oldValue == null && newValue == null)
                return; // unnecessary to forward here.

            store.put(key, newValue);
            tupleForwarder.maybeForward(key, newValue, oldValue);
        }

    }

    private class KTableFilterValueGetter implements KTableValueGetter<K, V> {

        private final KTableValueGetter<K, V> parentGetter;

        public KTableFilterValueGetter(KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init(ProcessorContext context) {
            parentGetter.init(context);
        }

        @Override
        public V get(K key) {
            return computeValue(key, parentGetter.get(key));
        }

    }

}
