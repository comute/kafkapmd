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

import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;

/**
 * A {@link StateStoreSupplier} that supports forwarding of values that have been
 * buffered in an {@link MemoryLRUCacheBytes}
 * @param <K>
 * @param <V>
 */
public interface ForwardingStateStoreSupplier<K, V> extends StateStoreSupplier {

    /**
     * Return a new {@link StateStore} instance that uses the passed in {@link CacheFlushListener}
     * when caching is enabled & the {@link MemoryLRUCacheBytes} is flushed
     * @param listener
     */
    StateStore get(final CacheFlushListener<K, V> listener);
}
