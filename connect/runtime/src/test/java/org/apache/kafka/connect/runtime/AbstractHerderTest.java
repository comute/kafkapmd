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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.PrincipalConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.connect.runtime.AbstractHerder.keysWithVariableValues;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.powermock.api.easymock.PowerMock.verifyAll;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AbstractHerder.class})
public class AbstractHerderTest {

    private static final String CONN1 = "sourceA";
    private static final ConnectorTaskId TASK0 = new ConnectorTaskId(CONN1, 0);
    private static final ConnectorTaskId TASK1 = new ConnectorTaskId(CONN1, 1);
    private static final ConnectorTaskId TASK2 = new ConnectorTaskId(CONN1, 2);
    private static final Integer MAX_TASKS = 3;
    private static final Map<String, String> CONN1_CONFIG = new HashMap<>();
    private static final String TEST_KEY = "testKey";
    private static final String TEST_KEY2 = "testKey2";
    private static final String TEST_KEY3 = "testKey3";
    private static final String TEST_VAL = "testVal";
    private static final String TEST_VAL2 = "testVal2";
    private static final String TEST_REF = "${file:/tmp/somefile.txt:somevar}";
    private static final String TEST_REF2 = "${file:/tmp/somefile2.txt:somevar2}";
    private static final String TEST_REF3 = "${file:/tmp/somefile3.txt:somevar3}";
    static {
        CONN1_CONFIG.put(ConnectorConfig.NAME_CONFIG, CONN1);
        CONN1_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONN1_CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        CONN1_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
        CONN1_CONFIG.put(TEST_KEY, TEST_REF);
        CONN1_CONFIG.put(TEST_KEY2, TEST_REF2);
        CONN1_CONFIG.put(TEST_KEY3, TEST_REF3);
    }
    private static final Map<String, String> TASK_CONFIG = new HashMap<>();
    static {
        TASK_CONFIG.put(TaskConfig.TASK_CLASS_CONFIG, BogusSourceTask.class.getName());
        TASK_CONFIG.put(TEST_KEY, TEST_REF);
    }
    private static final List<Map<String, String>> TASK_CONFIGS = new ArrayList<>();
    static {
        TASK_CONFIGS.add(TASK_CONFIG);
        TASK_CONFIGS.add(TASK_CONFIG);
        TASK_CONFIGS.add(TASK_CONFIG);
    }
    private static final HashMap<ConnectorTaskId, Map<String, String>> TASK_CONFIGS_MAP = new HashMap<>();
    static {
        TASK_CONFIGS_MAP.put(TASK0, TASK_CONFIG);
        TASK_CONFIGS_MAP.put(TASK1, TASK_CONFIG);
        TASK_CONFIGS_MAP.put(TASK2, TASK_CONFIG);
    }
    private static final ClusterConfigState SNAPSHOT = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.<String>emptySet());
    private static final ClusterConfigState SNAPSHOT_NO_TASKS = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            Collections.emptyMap(), Collections.<String>emptySet());

    private final String workerId = "workerId";
    private final String kafkaClusterId = "I4ZmrWqfT2e-upky_4fdPA";
    private final int generation = 5;
    private final String connector = "connector";
    private final ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();

    @MockStrict private Worker worker;
    @MockStrict private WorkerConfigTransformer transformer;
    @MockStrict private Plugins plugins;
    @MockStrict private ClassLoader classLoader;
    @MockStrict private ConfigBackingStore configStore;
    @MockStrict private StatusBackingStore statusStore;

    @Test
    public void testConnectors() {
        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
            .withConstructor(
                Worker.class,
                String.class,
                String.class,
                StatusBackingStore.class,
                ConfigBackingStore.class,
                ConnectorClientConfigOverridePolicy.class
            )
            .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
            .addMockedMethod("generation")
            .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.config(connector)).andReturn(null);
        EasyMock.expect(configStore.snapshot()).andReturn(SNAPSHOT);
        replayAll();
        assertEquals(Collections.singleton(CONN1), new HashSet<>(herder.connectors()));
        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connector, 0);
        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
            .withConstructor(
                Worker.class,
                String.class,
                String.class,
                StatusBackingStore.class,
                ConfigBackingStore.class,
                ConnectorClientConfigOverridePolicy.class
            )
            .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
            .addMockedMethod("generation")
            .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.config(connector)).andReturn(null);
        EasyMock.expect(statusStore.get(connector))
            .andReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));
        EasyMock.expect(statusStore.getAll(connector))
            .andReturn(Collections.singletonList(
                new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));

        replayAll();
        ConnectorStateInfo csi = herder.connectorStatus(connector);
        PowerMock.verifyAll();
    }

    @Test
    public void connectorStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connector, 0);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                                 ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(generation);
        EasyMock.expect(herder.config(connector)).andReturn(null);

        EasyMock.expect(statusStore.get(connector))
                .andReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        EasyMock.expect(statusStore.getAll(connector))
                .andReturn(Collections.singletonList(
                        new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);

        replayAll();


        ConnectorStateInfo state = herder.connectorStatus(connector);

        assertEquals(connector, state.name());
        assertEquals("RUNNING", state.connector().state());
        assertEquals(1, state.tasks().size());
        assertEquals(workerId, state.connector().workerId());

        ConnectorStateInfo.TaskState taskState = state.tasks().get(0);
        assertEquals(0, taskState.id());
        assertEquals("UNASSIGNED", taskState.state());
        assertEquals(workerId, taskState.workerId());

        PowerMock.verifyAll();
    }

    @Test
    public void taskStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId("connector", 0);
        String workerId = "workerId";

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                                 ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();

        EasyMock.expect(herder.generation()).andStubReturn(5);

        final Capture<TaskStatus> statusCapture = EasyMock.newCapture();
        statusStore.putSafe(EasyMock.capture(statusCapture));
        EasyMock.expectLastCall();

        EasyMock.expect(statusStore.get(taskId)).andAnswer(new IAnswer<TaskStatus>() {
            @Override
            public TaskStatus answer() throws Throwable {
                return statusCapture.getValue();
            }
        });

        replayAll();

        herder.onFailure(taskId, new RuntimeException());

        ConnectorStateInfo.TaskState taskState = herder.taskStatus(taskId);
        assertEquals(workerId, taskState.workerId());
        assertEquals("FAILED", taskState.state());
        assertEquals(0, taskState.id());
        assertNotNull(taskState.trace());

        verifyAll();
    }


    @Test(expected = BadRequestException.class)
    public void testConfigValidationEmptyConfig() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        herder.validateConnectorConfig(new HashMap<String, String>());

        verifyAll();
    }

    @Test()
    public void testConfigValidationMissingName() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        Map<String, String> config = Collections.singletonMap(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestSourceConnector.class.getName());
        ConfigInfos result = herder.validateConnectorConfig(config);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(TestSourceConnector.class.getName(), result.name());
        assertEquals(Arrays.asList(ConnectorConfig.COMMON_GROUP, ConnectorConfig.TRANSFORMS_GROUP, ConnectorConfig.ERROR_GROUP), result.groups());
        assertEquals(2, result.errorCount());
        // Base connector config has 13 fields, connector's configs add 2
        assertEquals(15, result.values().size());
        // Missing name should generate an error
        assertEquals(ConnectorConfig.NAME_CONFIG, result.values().get(0).configValue().name());
        assertEquals(1, result.values().get(0).configValue().errors().size());
        // "required" config from connector should generate an error
        assertEquals("required", result.values().get(13).configValue().name());
        assertEquals(1, result.values().get(13).configValue().errors().size());

        verifyAll();
    }

    @Test(expected = ConfigException.class)
    public void testConfigValidationInvalidTopics() {
        AbstractHerder herder = createConfigValidationHerder(TestSinkConnector.class, noneConnectorClientConfigOverridePolicy);
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestSinkConnector.class.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, "topic1,topic2");
        config.put(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "topic.*");

        herder.validateConnectorConfig(config);

        verifyAll();
    }

    @Test()
    public void testConfigValidationTransformsExtendResults() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class, noneConnectorClientConfigOverridePolicy);

        // 2 transform aliases defined -> 2 plugin lookups
        Set<PluginDesc<Transformation>> transformations = new HashSet<>();
        transformations.add(new PluginDesc<Transformation>(SampleTransformation.class, "1.0", classLoader));
        EasyMock.expect(plugins.transformations()).andReturn(transformations).times(2);

        replayAll();

        // Define 2 transformations. One has a class defined and so can get embedded configs, the other is missing
        // class info that should generate an error.
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestSourceConnector.class.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG, "xformA,xformB");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.type", SampleTransformation.class.getName());
        config.put("required", "value"); // connector required config
        ConfigInfos result = herder.validateConnectorConfig(config);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(TestSourceConnector.class.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
                ConnectorConfig.COMMON_GROUP,
                ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.ERROR_GROUP,
                "Transforms: xformA",
                "Transforms: xformB"
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(2, result.errorCount());
        // Base connector config has 13 fields, connector's configs add 2, 2 type fields from the transforms, and
        // 1 from the valid transformation's config
        assertEquals(18, result.values().size());
        // Should get 2 type fields from the transforms, first adds its own config since it has a valid class
        assertEquals("transforms.xformA.type", result.values().get(13).configValue().name());
        assertTrue(result.values().get(13).configValue().errors().isEmpty());
        assertEquals("transforms.xformA.subconfig", result.values().get(14).configValue().name());
        assertEquals("transforms.xformB.type", result.values().get(15).configValue().name());
        assertFalse(result.values().get(15).configValue().errors().isEmpty());

        verifyAll();
    }

    @Test()
    public void testConfigValidationPrincipalOnlyOverride() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class, new PrincipalConnectorClientConfigOverridePolicy());
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestSourceConnector.class.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put("required", "value"); // connector required config
        String ackConfigKey = producerOverrideKey(ProducerConfig.ACKS_CONFIG);
        String saslConfigKey = producerOverrideKey(SaslConfigs.SASL_JAAS_CONFIG);
        config.put(ackConfigKey, "none");
        config.put(saslConfigKey, "jaas_config");

        ConfigInfos result = herder.validateConnectorConfig(config);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to now allowed override policy for ACKS.... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(TestSourceConnector.class.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
            ConnectorConfig.COMMON_GROUP,
            ConnectorConfig.TRANSFORMS_GROUP,
            ConnectorConfig.ERROR_GROUP
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(1, result.errorCount());
        // Base connector config has 13 fields, connector's configs add 2, and 2 producer overrides
        assertEquals(17, result.values().size());
        assertTrue(result.values().stream().anyMatch(
            configInfo -> ackConfigKey.equals(configInfo.configValue().name()) && !configInfo.configValue().errors().isEmpty()));
        assertTrue(result.values().stream().anyMatch(
            configInfo -> saslConfigKey.equals(configInfo.configValue().name()) && configInfo.configValue().errors().isEmpty()));

        verifyAll();
    }

    @Test
    public void testConfigValidationAllOverride() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class, new AllConnectorClientConfigOverridePolicy());
        replayAll();

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestSourceConnector.class.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put("required", "value"); // connector required config
        // Try to test a variety of configuration types: string, int, long, boolean, list, class
        String protocolConfigKey = producerOverrideKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        config.put(protocolConfigKey, "SASL_PLAINTEXT");
        String maxRequestSizeConfigKey = producerOverrideKey(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        config.put(maxRequestSizeConfigKey, "420");
        String maxBlockConfigKey = producerOverrideKey(ProducerConfig.MAX_BLOCK_MS_CONFIG);
        config.put(maxBlockConfigKey, "28980");
        String idempotenceConfigKey = producerOverrideKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        config.put(idempotenceConfigKey, "true");
        String bootstrapServersConfigKey = producerOverrideKey(BOOTSTRAP_SERVERS_CONFIG);
        config.put(bootstrapServersConfigKey, "SASL_PLAINTEXT://localhost:12345,SASL_PLAINTEXT://localhost:23456");
        String loginCallbackHandlerConfigKey = producerOverrideKey(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        config.put(loginCallbackHandlerConfigKey, OAuthBearerUnsecuredLoginCallbackHandler.class.getName());

        final Set<String> overriddenClientConfigs = new HashSet<>();
        overriddenClientConfigs.add(protocolConfigKey);
        overriddenClientConfigs.add(maxRequestSizeConfigKey);
        overriddenClientConfigs.add(maxBlockConfigKey);
        overriddenClientConfigs.add(idempotenceConfigKey);
        overriddenClientConfigs.add(bootstrapServersConfigKey);
        overriddenClientConfigs.add(loginCallbackHandlerConfigKey);

        ConfigInfos result = herder.validateConnectorConfig(config);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        Map<String, String> validatedOverriddenClientConfigs = new HashMap<>();
        for (ConfigInfo configInfo : result.values()) {
            String configName = configInfo.configKey().name();
            if (overriddenClientConfigs.contains(configName)) {
                validatedOverriddenClientConfigs.put(configName, configInfo.configValue().value());
            }
        }
        Map<String, String> rawOverriddenClientConfigs = config.entrySet().stream()
            .filter(e -> overriddenClientConfigs.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertEquals(rawOverriddenClientConfigs, validatedOverriddenClientConfigs);
        verifyAll();
    }

    @Test
    public void testReverseTransformConfigs() {
        // Construct a task config with constant values for TEST_KEY and TEST_KEY2
        Map<String, String> newTaskConfig = new HashMap<>();
        newTaskConfig.put(TaskConfig.TASK_CLASS_CONFIG, BogusSourceTask.class.getName());
        newTaskConfig.put(TEST_KEY, TEST_VAL);
        newTaskConfig.put(TEST_KEY2, TEST_VAL2);
        List<Map<String, String>> newTaskConfigs = new ArrayList<>();
        newTaskConfigs.add(newTaskConfig);

        // The SNAPSHOT has a task config with TEST_KEY and TEST_REF
        List<Map<String, String>> reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT, newTaskConfigs);
        assertEquals(TEST_REF, reverseTransformed.get(0).get(TEST_KEY));

        // The SNAPSHOT has no task configs but does have a connector config with TEST_KEY2 and TEST_REF2
        reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT_NO_TASKS, newTaskConfigs);
        assertEquals(TEST_REF2, reverseTransformed.get(0).get(TEST_KEY2));

        // The reverseTransformed result should not have TEST_KEY3 since newTaskConfigs does not have TEST_KEY3
        reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT_NO_TASKS, newTaskConfigs);
        assertFalse(reverseTransformed.get(0).containsKey(TEST_KEY3));
    }

    @Test
    public void testConfigProviderRegex() {
        testConfigProviderRegex("\"${::}\"");
        testConfigProviderRegex("${::}");
        testConfigProviderRegex("\"${:/a:somevar}\"");
        testConfigProviderRegex("\"${file::somevar}\"");
        testConfigProviderRegex("${file:/a/b/c:}");
        testConfigProviderRegex("${file:/tmp/somefile.txt:somevar}");
        testConfigProviderRegex("\"${file:/tmp/somefile.txt:somevar}\"");
        testConfigProviderRegex("plain.PlainLoginModule required username=\"${file:/tmp/somefile.txt:somevar}\"");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar}");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar} not null");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar} password=${file:/tmp/somefile.txt:othervar}");
        testConfigProviderRegex("plain.PlainLoginModule required username", false);
    }

    @Test
    public void testConverterAndClientConfigExtraction() {
        Map<String, String> connProps = new HashMap<>(CONN1_CONFIG);
        assertEquals(Collections.emptyMap(), AbstractHerder.clientAndConverterConfigs(connProps));

        Map<String, String> converterAndClientProps = new HashMap<>();
        converterAndClientProps.put(KEY_CONVERTER_CLASS_CONFIG, "Rot13Converter");
        converterAndClientProps.put(KEY_CONVERTER_CLASS_CONFIG + ".crowded.train", "true");
        converterAndClientProps.put(VALUE_CONVERTER_CLASS_CONFIG, "AesonConverter");
        converterAndClientProps.put(VALUE_CONVERTER_CLASS_CONFIG + ".hs", "100");
        converterAndClientProps.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, "http://localhost:4761");
        converterAndClientProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BATCH_SIZE_CONFIG, "42069");
        converterAndClientProps.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + CLIENT_ID_CONFIG, "boofar");
        connProps.putAll(converterAndClientProps);
        assertEquals(converterAndClientProps, AbstractHerder.clientAndConverterConfigs(connProps));
    }

    private void testConfigProviderRegex(String rawConnConfig) {
        testConfigProviderRegex(rawConnConfig, true);
    }
    
    private void testConfigProviderRegex(String rawConnConfig, boolean expected) {
        Set<String> keys = keysWithVariableValues(Collections.singletonMap("key", rawConnConfig), ConfigTransformer.DEFAULT_PATTERN);
        boolean actual = keys != null && !keys.isEmpty() && keys.contains("key");
        assertEquals(String.format("%s should have matched regex", rawConnConfig), expected, actual);
    }

    private AbstractHerder createConfigValidationHerder(Class<? extends Connector> connectorClass,
                                                        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {


        ConfigBackingStore configStore = strictMock(ConfigBackingStore.class);
        StatusBackingStore statusStore = strictMock(StatusBackingStore.class);

        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
                .withConstructor(Worker.class, String.class, String.class, StatusBackingStore.class, ConfigBackingStore.class,
                                 ConnectorClientConfigOverridePolicy.class)
                .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, connectorClientConfigOverridePolicy)
                .addMockedMethod("generation")
                .createMock();
        EasyMock.expect(herder.generation()).andStubReturn(generation);

        // Call to validateConnectorConfig
        EasyMock.expect(worker.configTransformer()).andReturn(transformer).times(2);
        final Capture<Map<String, String>> configCapture = EasyMock.newCapture();
        EasyMock.expect(transformer.transform(EasyMock.capture(configCapture))).andAnswer(configCapture::getValue);
        EasyMock.expect(worker.getPlugins()).andStubReturn(plugins);
        final Connector connector;
        try {
            connector = connectorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Couldn't create connector", e);
        }
        EasyMock.expect(plugins.newConnector(connectorClass.getName())).andReturn(connector);
        EasyMock.expect(plugins.compareAndSwapLoaders(connector)).andReturn(classLoader);
        return herder;
    }

    public static class SampleTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
        @Override
        public void configure(Map<String, ?> configs) {

        }

        @Override
        public R apply(R record) {
            return record;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                           .define("subconfig", ConfigDef.Type.STRING, "default", ConfigDef.Importance.LOW, "docs");
        }

        @Override
        public void close() {

        }
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

    private static String producerOverrideKey(String config) {
        return ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + config;
    }
}
