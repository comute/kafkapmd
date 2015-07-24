/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.Configurable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * <p>
 * OffsetBackingStore is an interface for storage backends that store key-value data. The backing
 * store doesn't need to handle serialization or deserialization. It only needs to support
 * reading/writing bytes. Since it is expected these operations will require network
 * operations, only bulk operations are supported.
 * </p>
 * <p>
 * Since OffsetBackingStore is a shared resource that may be used by many OffsetStorage instances
 * that are associated with individual tasks, all operations include a namespace which should be
 * used to isolate different key spaces.
 * </p>
 */
public interface OffsetBackingStore extends Configurable {

    /**
     * Start this offset store.
     */
    public void start();

    /**
     * Stop the backing store. Implementations should attempt to shutdown gracefully, but not block
     * indefinitely.
     */
    public void stop();

    /**
     * Get the values for the specified keys
     * @param namespace prefix for the keys in this request
     * @param keys list of keys to look up
     * @param callback callback to invoke on completion
     * @return future for the resulting map from key to value
     */
    public Future<Map<ByteBuffer, ByteBuffer>> get(
            String namespace, Collection<ByteBuffer> keys,
            Callback<Map<ByteBuffer, ByteBuffer>> callback);

    /**
     * Set the specified keys and values.
     * @param namespace prefix for the keys in this request
     * @param values map from key to value
     * @param callback callback to invoke on completion
     * @return void future for the operation
     */
    public Future<Void> set(String namespace, Map<ByteBuffer, ByteBuffer> values,
                            Callback<Void> callback);
}
