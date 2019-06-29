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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SubscriptionResponseWrapperSerde<V> implements Serde<SubscriptionResponseWrapper<V>> {
    private final SubscriptionResponseWrapperSerializer<V> serializer;
    private final SubscriptionResponseWrapperDeserializer<V> deserializer;
    public static int VERSION_BITS = 7;

    public SubscriptionResponseWrapperSerde(final Serde<V> foreignValueSerde) {
        this.serializer = new SubscriptionResponseWrapperSerializer<>(foreignValueSerde.serializer());
        this.deserializer = new SubscriptionResponseWrapperDeserializer<>(foreignValueSerde.deserializer());
    }

    @Override
    public void configure(final Map configs, final boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<SubscriptionResponseWrapper<V>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<SubscriptionResponseWrapper<V>> deserializer() {
        return deserializer;
    }

    private static class SubscriptionResponseWrapperSerializer<V> implements Serializer<SubscriptionResponseWrapper<V>> {
        private final Serializer<V> serializer;

        public SubscriptionResponseWrapperSerializer(final Serializer<V> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void configure(final Map configs, final boolean isKey) {
            //Do nothing
        }

        @Override
        public byte[] serialize(final String topic, final SubscriptionResponseWrapper<V> data) {
            //{1-bit-isHashNull}{7-bits-version}{Optional-16-byte-Hash}{n-bytes serialized data}

            final byte[] serializedData = serializer.serialize(topic, data.getForeignValue());
            final int serializedDataLength = serializedData == null ? 0 : serializedData.length;
            final long[] originalHash = data.getOriginalValueHash();
            final int hashLength = originalHash == null ? 0 : 2 * Long.BYTES;

            final ByteBuffer buf = ByteBuffer.allocate(1 + hashLength + serializedDataLength);

            if (originalHash != null) {
                buf.put((byte) (data.getVersion() | (byte) 0x00 ));
                buf.putLong(originalHash[0]);
                buf.putLong(originalHash[1]);
            } else {
                //Don't store hash as it's null.
                buf.put((byte) (data.getVersion() | (byte) 0x80 ));
            }

            if (serializedData != null)
                buf.put(serializedData);
            return buf.array();
        }

        @Override
        public void close() {
            //Do nothing
        }
    }

    private static class SubscriptionResponseWrapperDeserializer<V> implements Deserializer<SubscriptionResponseWrapper<V>> {
        final private Deserializer<V> deserializer;

        public SubscriptionResponseWrapperDeserializer(final Deserializer<V> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            //Do nothing
        }

        @Override
        public SubscriptionResponseWrapper<V> deserialize(final String topic, final byte[] data) {
            //{1-bit-isHashNull}{7-bits-version}{Optional-16-byte-Hash}{n-bytes serialized data}

            final ByteBuffer buf = ByteBuffer.wrap(data);
            final byte versionAndIsHashNull = buf.get();
            final byte version = (byte)(0x7F & versionAndIsHashNull);
            final boolean isHashNull = (0x80 & versionAndIsHashNull) == 0x80;

            final long[] hash;
            int lengthSum = 1; //The first byte
            if (isHashNull) {
                hash = null;
            } else {
                hash = new long[2];
                hash[0] = buf.getLong();
                hash[1] = buf.getLong();
                lengthSum += 2 * Long.BYTES;
            }

            final byte[] serializedValue;
            if (data.length - lengthSum > 0) {
                serializedValue = new byte[data.length - lengthSum];
                buf.get(serializedValue, 0, serializedValue.length);
            } else
                serializedValue = null;

            final V value = deserializer.deserialize(topic, serializedValue);
            return new SubscriptionResponseWrapper<>(hash, value, version);
        }

        @Override
        public void close() {
            //Do nothing
        }
    }

}
