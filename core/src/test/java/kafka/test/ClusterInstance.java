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

package kafka.test;

import kafka.api.IntegrationTestHarness;
import kafka.log.UnifiedLog;
import kafka.network.SocketServer;
import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;
import kafka.server.checkpoints.OffsetCheckpointFile;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.Type;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.test.TestUtils;
import scala.collection.JavaConverters;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.consumer.GroupProtocol.CLASSIC;
import static org.apache.kafka.clients.consumer.GroupProtocol.CONSUMER;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG;

public interface ClusterInstance {

    Type type();

    default boolean isKRaftTest() {
        return type() == Type.KRAFT || type() == Type.CO_KRAFT;
    }

    Map<Integer, KafkaBroker> brokers();

    default Map<Integer, KafkaBroker> aliveBrokers() {
        return brokers().entrySet().stream().filter(entry -> !entry.getValue().isShutdown())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Map<Integer, ControllerServer> controllers();

    /**
     * The immutable cluster configuration used to create this cluster.
     */
    ClusterConfig config();

    /**
     * Return the set of all controller IDs configured for this test. For kraft, this
     * will return only the nodes which have the "controller" role enabled in `process.roles`.
     * For zookeeper, this will return all broker IDs since they are all eligible controllers.
     */
    Set<Integer> controllerIds();

    /**
     * Return the set of all broker IDs configured for this test.
     */
    default Set<Integer> brokerIds() {
        return brokers().keySet();
    }

    /**
     * The listener for this cluster as configured by {@link ClusterTest} or by {@link ClusterConfig}. If
     * unspecified by those sources, this will return the listener for the default security protocol PLAINTEXT
     */
    ListenerName clientListener();

    /**
     * The listener for the kraft cluster controller configured by controller.listener.names. In ZK-based clusters, return Optional.empty
     */
    default Optional<ListenerName> controllerListenerName() {
        return Optional.empty();
    }

    /**
     * The listener for the zk controller configured by control.plane.listener.name. In Raft-based clusters, return Optional.empty
     */
    default Optional<ListenerName> controlPlaneListenerName() {
        return Optional.empty();
    }

    /**
     * The broker connect string which can be used by clients for bootstrapping
     */
    String bootstrapServers();

    /**
     * The broker connect string which can be used by clients for bootstrapping to the controller quorum.
     */
    String bootstrapControllers();

    /**
     * A collection of all brokers in the cluster. In ZK-based clusters this will also include the broker which is
     * acting as the controller (since ZK controllers serve both broker and controller roles).
     */
    default Collection<SocketServer> brokerSocketServers() {
        return brokers().values().stream()
                .map(KafkaBroker::socketServer)
                .collect(Collectors.toList());
    }

    /**
     * A collection of all controllers in the cluster. For ZK-based clusters, this will return the broker which is also
     * currently the active controller. For Raft-based clusters, this will return all controller servers.
     */
    Collection<SocketServer> controllerSocketServers();

    /**
     * Return any one of the broker servers. Throw an error if none are found
     */
    default SocketServer anyBrokerSocketServer() {
        return brokerSocketServers().stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No broker SocketServers found"));
    }

    /**
     * Return any one of the controller servers. Throw an error if none are found
     */
    default SocketServer anyControllerSocketServer() {
        return controllerSocketServers().stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No controller SocketServers found"));
    }

    String clusterId();

    /**
     * The underlying object which is responsible for setting up and tearing down the cluster.
     */
    Object getUnderlying();

    default <T> T getUnderlying(Class<T> asClass) {
        return asClass.cast(getUnderlying());
    }

    Admin createAdminClient(Properties configOverrides);

    default Admin createAdminClient() {
        return createAdminClient(new Properties());
    }

    default Set<GroupProtocol> supportedGroupProtocols() {
        Map<String, String> serverProperties = config().serverProperties();
        Set<GroupProtocol> supportedGroupProtocols = new HashSet<>();
        supportedGroupProtocols.add(CLASSIC);

        if (serverProperties.getOrDefault(GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "").contains("consumer")) {
            supportedGroupProtocols.add(CONSUMER);
        }

        return Collections.unmodifiableSet(supportedGroupProtocols);
    }

    //---------------------------[modify]---------------------------//

    void start();

    void stop();

    void shutdownBroker(int brokerId);

    void startBroker(int brokerId);

    //---------------------------[wait]---------------------------//

    @SuppressWarnings("deprecation")
    default void verifyTopicDeletion(String topic, int partitions) throws InterruptedException {
        if (!isKRaftTest()) {
            TestUtils.waitForCondition(
                    () -> !((IntegrationTestHarness) getUnderlying()).zkClient().isTopicMarkedForDeletion(topic),
                    String.format("Admin path /admin/delete_topics/%s path not deleted even after a replica is restarted", topic)
            );

            TestUtils.waitForCondition(
                    () -> !((IntegrationTestHarness) getUnderlying()).zkClient().topicExists(topic),
                    String.format("Topic path /brokers/topics/%s not deleted after /admin/delete_topics/%s path is deleted", topic, topic)
            );
        }

        List<TopicPartition> topicPartitions = IntStream.range(0, partitions).mapToObj(i -> new TopicPartition(topic, i)).collect(Collectors.toList());
        TestUtils.waitForCondition(
                () ->
                        brokers().values().stream().allMatch(broker ->
                                topicPartitions.stream().allMatch(tp ->
                                        broker.replicaManager().onlinePartition(tp).isEmpty())
                        ), "Replica manager's should have deleted all of this topic's partitions"
        );

        TestUtils.waitForCondition(
                () ->
                        brokers().values().stream().allMatch(broker ->
                                topicPartitions.stream().allMatch(tp ->
                                        broker.replicaManager().onlinePartition(tp).isEmpty())
                        ), "Replica logs not deleted after delete topic is complete"
        );


        TestUtils.waitForCondition(() -> brokers().values().stream().allMatch(broker ->
                topicPartitions.stream().allMatch(tp ->
                        JavaConverters.seqAsJavaList(broker.logManager().liveLogDirs()).stream()
                                .map(logDir -> JavaConverters.mapAsJavaMap(new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null).read()))
                                .collect(Collectors.toList())
                                .stream()
                                .noneMatch(checkpointsPerLogDir -> checkpointsPerLogDir.containsKey(tp))
                )
        ), "Cleaner offset for deleted partition should have been removed");

        TestUtils.waitForCondition(() -> brokers().values().stream().allMatch(
                        broker -> broker.config().logDirs().forall(
                                logDir -> topicPartitions.stream().noneMatch(
                                        tp -> new File(logDir, tp.topic() + "-" + tp.partition()).exists()))),
                "Failed to soft-delete the data to a delete directory"
        );

        TestUtils.waitForCondition(() -> brokers().values().stream().allMatch(
                broker -> broker.config().logDirs().forall(
                        logDir -> topicPartitions.stream().noneMatch(
                                tp -> Arrays.asList(new File(logDir).list()).stream().allMatch(
                                        partitionDirectoryName -> partitionDirectoryName.startsWith(tp.topic() + "-" + tp.partition()) &&
                                                partitionDirectoryName.endsWith(UnifiedLog.DeleteDirSuffix())
                                )
                        )
                )
        ), "Failed to hard-delete the delete directory");
    }

    void waitForReadyBrokers() throws InterruptedException;

    default void waitForTopic(String topic, int partitions) throws InterruptedException {
        // wait for metadata
        TestUtils.waitForCondition(
            () -> brokers().values().stream().allMatch(broker -> partitions == 0 ?
                broker.metadataCache().numPartitions(topic).isEmpty() :
                broker.metadataCache().numPartitions(topic).contains(partitions)
        ), 60000L, topic + " metadata not propagated after 60000 ms");

        for (ControllerServer controller : controllers().values()) {
            long controllerOffset = controller.raftManager().replicatedLog().endOffset().offset() - 1;
            TestUtils.waitForCondition(
                () -> brokers().values().stream().allMatch(broker -> ((BrokerServer) broker).sharedServer().loader().lastAppliedOffset() >= controllerOffset),
                60000L, "Timeout waiting for controller metadata propagating to brokers");
        }
    }
}
