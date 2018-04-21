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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamMapTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Test
    public void testMap() {
        StreamsBuilder builder = new StreamsBuilder();

        KeyValueMapper<Integer, String, KeyValue<String, Integer>> mapper =
            new KeyValueMapper<Integer, String, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(Integer key, String value) {
                    return KeyValue.pair(value, key);
                }
            };

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(intSerde, stringSerde));
        MockProcessorSupplier<String, Integer> supplier;

        supplier = new MockProcessorSupplier<>();
        stream.map(mapper).process(supplier);

        driver.setUp(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, "V" + expectedKey);
        }

        assertEquals(4, supplier.getTheProcessor().processed.size());

        String[] expected = new String[]{"V0:0", "V1:1", "V2:2", "V3:3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], supplier.getTheProcessor().processed.get(i));
        }
    }

    @Test
    public void testTypeVariance() {
        KeyValueMapper<Number, Object, KeyValue<Number, String>> stringify = new KeyValueMapper<Number, Object, KeyValue<Number, String>>() {
            @Override
            public KeyValue<Number, String> apply(Number key, Object value) {
                return KeyValue.pair(key, key + ":" + value);
            }
        };

        new StreamsBuilder()
            .<Integer, String>stream("numbers")
            .map(stringify)
            .to("strings");
    }
}
