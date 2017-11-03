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
package org.apache.kafka.common.serialization;

import java.util.Map;

/**
 * A Serializer with empty {@code configure()} and {@code close()} methods.
 *
 * Prefer {@link Serializer} if both {@code configure()} and {@code close()}
 * methods are needed to be non-empty.
 *
 * Once Kafka drops support for Java 7, the {@code configure()} and
 * {@code close()} methods will be implemented as default empty methods in
 * {@link Serializer} so that backwards compatibility is maintained. This class
 * may be deprecated once that happens.
 */
public abstract class NoConfSerializer<T> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public void close() {
        // nothing to do
    }
}
