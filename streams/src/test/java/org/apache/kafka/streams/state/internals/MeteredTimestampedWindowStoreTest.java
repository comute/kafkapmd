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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MeteredTimestampedWindowStoreTest {

    private static final String STORE_NAME = "mocked-store";
    private static final String STORE_TYPE = "scope";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    // timestamp is 97 what is ASCII of 'a'
    private static final long TIMESTAMP = 97L;
    private static final ValueAndTimestamp<String> VALUE_AND_TIMESTAMP =
        ValueAndTimestamp.make("value", TIMESTAMP);
    private static final byte[] VALUE_AND_TIMESTAMP_BYTES = "\0\0\0\0\0\0\0avalue".getBytes();
    private static final int WINDOW_SIZE_MS = 10;

    private InternalMockProcessorContext context;
    private final Serde<String> keySerde = niceMock(Serde.class);
    private final Serializer<String> keySerializer = mock(Serializer.class);
    private final Serde<ValueAndTimestamp<String>> valueSerde = niceMock(Serde.class);
    private final Deserializer<ValueAndTimestamp<String>> valueDeserializer = mock(Deserializer.class);
    private final Serializer<ValueAndTimestamp<String>> valueSerializer = mock(Serializer.class);
    private final WindowStore<Bytes, byte[]> innerStoreMock = EasyMock.createNiceMock(WindowStore.class);
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private MeteredTimestampedWindowStore<String, String> store = new MeteredTimestampedWindowStore<>(
        innerStoreMock,
        WINDOW_SIZE_MS, // any size
        STORE_TYPE,
        new MockTime(),
        Serdes.String(),
        new ValueAndTimestampSerde<>(new SerdeThatDoesntHandleNull())
    );

    {
        EasyMock.expect(innerStoreMock.name()).andStubReturn(STORE_NAME);
    }

    @Before
    public void setUp() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", StreamsConfig.METRICS_LATEST);

        context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics)
        );
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        context.addChangelogForStore(STORE_NAME, CHANGELOG_TOPIC);
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        final String defaultChangelogTopicName =
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), STORE_NAME);
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
    }

    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        expect(keySerde.serializer()).andStubReturn(keySerializer);
        expect(keySerializer.serialize(topic, KEY)).andStubReturn(KEY.getBytes());
        expect(valueSerde.deserializer()).andStubReturn(valueDeserializer);
        expect(valueDeserializer.deserialize(topic, VALUE_AND_TIMESTAMP_BYTES)).andStubReturn(VALUE_AND_TIMESTAMP);
        expect(valueSerde.serializer()).andStubReturn(valueSerializer);
        expect(valueSerializer.serialize(topic, VALUE_AND_TIMESTAMP)).andStubReturn(VALUE_AND_TIMESTAMP_BYTES);
        expect(innerStoreMock.fetch(KEY_BYTES, TIMESTAMP)).andStubReturn(VALUE_AND_TIMESTAMP_BYTES);
        replay(innerStoreMock, keySerializer, keySerde, valueDeserializer, valueSerializer, valueSerde);
        store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            WINDOW_SIZE_MS,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );
        store.init(context, store);

        store.fetch(KEY, TIMESTAMP);
        store.put(KEY, VALUE_AND_TIMESTAMP, TIMESTAMP);

        verify(keySerializer, valueDeserializer, valueSerializer);
    }

    @Test
    public void shouldCloseUnderlyingStore() {
        innerStoreMock.close();
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.close();
        EasyMock.verify(innerStoreMock);
    }

    @Test
    public void shouldNotExceptionIfFetchReturnsNull() {
        EasyMock.expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).andReturn(null);
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        assertNull(store.fetch("a", 0));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext() {
        EasyMock.expect(innerStoreMock.name()).andStubReturn("mocked-store");
        EasyMock.replay(innerStoreMock);
        final MeteredTimestampedWindowStore<String, Long> store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            null,
            null
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from processor context.");
            }
            throw exception;
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters() {
        EasyMock.expect(innerStoreMock.name()).andStubReturn("mocked-store");
        EasyMock.replay(innerStoreMock);
        final MeteredTimestampedWindowStore<String, Long> store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.Long())
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from constructor parameters.");
            }
            throw exception;
        }
    }
}
