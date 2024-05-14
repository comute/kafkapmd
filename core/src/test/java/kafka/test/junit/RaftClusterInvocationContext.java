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

package kafka.test.junit;

import kafka.network.SocketServer;
import kafka.server.BrokerFeatures;
import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.test.annotation.Type;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerState;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import scala.compat.java8.OptionConverters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wraps a {@link KafkaClusterTestKit} inside lifecycle methods for a test invocation. Each instance of this
 * class is provided with a configuration for the cluster.
 *
 * This context also provides parameter resolvers for:
 *
 * <ul>
 *     <li>ClusterConfig (the same instance passed to the constructor)</li>
 *     <li>ClusterInstance (includes methods to expose underlying SocketServer-s)</li>
 * </ul>
 */
public class RaftClusterInvocationContext implements TestTemplateInvocationContext {

    private final String baseDisplayName;
    private final ClusterConfig clusterConfig;
    private final boolean isCombined;

    public RaftClusterInvocationContext(String baseDisplayName, ClusterConfig clusterConfig, boolean isCombined) {
        this.baseDisplayName = baseDisplayName;
        this.clusterConfig = clusterConfig;
        this.isCombined = isCombined;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        String clusterDesc = clusterConfig.nameTags().entrySet().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        return String.format("%s [%d] Type=Raft-%s, %s", baseDisplayName, invocationIndex, isCombined ? "Combined" : "Isolated", clusterDesc);
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        RaftClusterInstance clusterInstance = new RaftClusterInstance(clusterConfig, isCombined);
        return Arrays.asList(
                (BeforeTestExecutionCallback) context -> {
                    if (clusterConfig.isAutoStart()) {
                        clusterInstance.start();
                    }
                },
                (AfterTestExecutionCallback) context -> clusterInstance.stop(),
                new ClusterInstanceParameterResolver(clusterInstance)
        );
    }

    public static class RaftClusterInstance implements ClusterInstance {

        private final ClusterConfig clusterConfig;
        final AtomicBoolean started = new AtomicBoolean(false);
        final AtomicBoolean stopped = new AtomicBoolean(false);
        private final ConcurrentLinkedQueue<Admin> admins = new ConcurrentLinkedQueue<>();
        private EmbeddedZookeeper embeddedZookeeper;
        private KafkaClusterTestKit clusterTestKit;
        private final boolean isCombined;

        RaftClusterInstance(ClusterConfig clusterConfig, boolean isCombined) {
            this.clusterConfig = clusterConfig;
            this.isCombined = isCombined;
        }

        @Override
        public String bootstrapServers() {
            return clusterTestKit.bootstrapServers();
        }

        @Override
        public String bootstrapControllers() {
            return clusterTestKit.bootstrapControllers();
        }

        @Override
        public Collection<SocketServer> brokerSocketServers() {
            return brokers()
                    .map(BrokerServer::socketServer)
                    .collect(Collectors.toList());
        }

        @Override
        public ListenerName clientListener() {
            return ListenerName.normalised("EXTERNAL");
        }

        @Override
        public Optional<ListenerName> controllerListenerName() {
            return OptionConverters.toJava(controllers().findAny().get().config().controllerListenerNames().headOption().map(ListenerName::new));
        }

        @Override
        public Collection<SocketServer> controllerSocketServers() {
            return controllers()
                    .map(ControllerServer::socketServer)
                    .collect(Collectors.toList());
        }

        @Override
        public SocketServer anyBrokerSocketServer() {
            return brokers()
                    .map(BrokerServer::socketServer)
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No broker SocketServers found"));
        }

        @Override
        public SocketServer anyControllerSocketServer() {
            return controllers()
                    .map(ControllerServer::socketServer)
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No controller SocketServers found"));
        }

        @Override
        public Map<Integer, BrokerFeatures> brokerFeatures() {
            return brokers().collect(Collectors.toMap(
                    brokerServer -> brokerServer.config().nodeId(),
                    BrokerServer::brokerFeatures
            ));
        }

        @Override
        public String clusterId() {
            return controllers().findFirst().map(ControllerServer::clusterId).orElse(
                    brokers().findFirst().map(BrokerServer::clusterId).orElseThrow(
                            () -> new RuntimeException("No controllers or brokers!"))
            );
        }

        public Collection<ControllerServer> controllerServers() {
            return controllers().collect(Collectors.toList());
        }

        @Override
        public Type type() {
            return isCombined ? Type.CO_KRAFT : Type.KRAFT;
        }

        @Override
        public ClusterConfig config() {
            return clusterConfig;
        }

        @Override
        public Set<Integer> controllerIds() {
            return controllers()
                    .map(controllerServer -> controllerServer.config().nodeId())
                    .collect(Collectors.toSet());
        }

        @Override
        public Set<Integer> brokerIds() {
            return brokers()
                    .map(brokerServer -> brokerServer.config().nodeId())
                    .collect(Collectors.toSet());
        }

        @Override
        public KafkaClusterTestKit getUnderlying() {
            return clusterTestKit;
        }

        @Override
        public Admin createAdminClient(Properties configOverrides) {
            Admin admin = Admin.create(clusterTestKit.newClientPropertiesBuilder(configOverrides).build());
            admins.add(admin);
            return admin;
        }

        @Override
        public void start() {
            if (started.compareAndSet(false, true)) {
                try {
                    buildAndFormatCluster();
                    clusterTestKit.startup();
                    kafka.utils.TestUtils.waitUntilTrue(
                            () -> this.clusterTestKit.brokers().get(0).brokerState() == BrokerState.RUNNING,
                            () -> "Broker never made it to RUNNING state.",
                            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                            100L);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start Raft server", e);
                }
            }
        }

        @Override
        public void stop() {
            if (stopped.compareAndSet(false, true)) {
                admins.forEach(admin -> Utils.closeQuietly(admin, "admin"));
                admins.clear();
                Utils.closeQuietly(clusterTestKit, "cluster");
                if (embeddedZookeeper != null) {
                    Utils.closeQuietly(embeddedZookeeper, "zk");
                }
            }
        }

        @Override
        public void shutdownBroker(int brokerId) {
            findBrokerOrThrow(brokerId).shutdown();
        }

        @Override
        public void startBroker(int brokerId) {
            findBrokerOrThrow(brokerId).startup();
        }

        @Override
        public void waitForReadyBrokers() throws InterruptedException {
            try {
                clusterTestKit.waitForReadyBrokers();
            } catch (ExecutionException e) {
                throw new AssertionError("Failed while waiting for brokers to become ready", e);
            }
        }

        public Stream<BrokerServer> brokers() {
            return clusterTestKit.brokers().values().stream();
        }

        public Stream<ControllerServer> controllers() {
            return clusterTestKit.controllers().values().stream();
        }

        private BrokerServer findBrokerOrThrow(int brokerId) {
            return Optional.ofNullable(clusterTestKit.brokers().get(brokerId))
                    .orElseThrow(() -> new IllegalArgumentException("Unknown brokerId " + brokerId));
        }

        private void buildAndFormatCluster() throws Exception {
            TestKitNodes nodes = new TestKitNodes.Builder()
                    .setBootstrapMetadataVersion(clusterConfig.metadataVersion())
                    .setCombined(isCombined)
                    .setNumBrokerNodes(clusterConfig.numBrokers())
                    .setNumDisksPerBroker(clusterConfig.numDisksPerBroker())
                    .setPerServerProperties(clusterConfig.perServerOverrideProperties())
                    .setNumControllerNodes(clusterConfig.numControllers()).build();
            KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
            if (Boolean.parseBoolean(clusterConfig.serverProperties()
                    .getOrDefault("zookeeper.metadata.migration.enable", "false"))) {
                this.embeddedZookeeper = new EmbeddedZookeeper();
                builder.setConfigProp("zookeeper.connect", String.format("localhost:%d", embeddedZookeeper.port()));
            }
            // Copy properties into the TestKit builder
            clusterConfig.serverProperties().forEach(builder::setConfigProp);
            // KAFKA-12512 need to pass security protocol and listener name here
            this.clusterTestKit = builder.build();
            this.clusterTestKit.format();
        }
    }
}
