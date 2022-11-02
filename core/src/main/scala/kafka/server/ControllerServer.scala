/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util
import java.util.OptionalLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CompletableFuture, TimeUnit}
import kafka.cluster.Broker.ServerInfo
import kafka.metrics.{KafkaMetricsGroup, LinuxIoMetricsCollector}
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.raft.RaftManager
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig.{AlterConfigPolicyClassNameProp, CreateTopicPolicyClassNameProp}
import kafka.server.KafkaRaftServer.BrokerRole
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.DynamicConfigPublisher
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Endpoint}
import org.apache.kafka.controller.{Controller, ControllerMetrics, QuorumController, QuorumFeatures}
import org.apache.kafka.metadata.KafkaConfigSchema
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.raft.RaftConfig.AddressSpec
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.image.loader.MetadataLoader
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata
import org.apache.kafka.server.fault.FaultHandler
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}

import scala.collection.{Map, immutable}
import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._

/**
 * A Kafka controller that runs in KRaft (Kafka Raft) mode.
 */
class ControllerServer(
  val metaProperties: MetaProperties,
  val config: KafkaConfig,
  val raftManager: RaftManager[ApiMessageAndVersion],
  val time: Time,
  val metrics: Metrics,
  val controllerMetrics: ControllerMetrics,
  val threadNamePrefix: Option[String],
  val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]],
  val configSchema: KafkaConfigSchema,
  val raftApiVersions: ApiVersions,
  val bootstrapMetadata: BootstrapMetadata,
  val metadataFaultHandler: FaultHandler,
  val fatalFaultHandler: FaultHandler,
  val metadataLoader: MetadataLoader,
) extends Logging with KafkaMetricsGroup {
  import kafka.server.Server._

  config.dynamicConfig.initialize(zkClientOpt = None)

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  var linuxIoMetricsCollector: LinuxIoMetricsCollector = _
  @volatile var authorizer: Option[Authorizer] = None
  var tokenCache: DelegationTokenCache = _
  var credentialProvider: CredentialProvider = _
  var socketServer: SocketServer = _
  val socketServerFirstBoundPortFuture = new CompletableFuture[Integer]()
  var createTopicPolicy: Option[CreateTopicPolicy] = None
  var alterConfigPolicy: Option[AlterConfigPolicy] = None
  var controller: Controller = _
  var quotaManagers: QuotaManagers = _
  var controllerApis: ControllerApis = _
  var controllerApisHandlerPool: KafkaRequestHandlerPool = _
  var dynamicConfigPublisher: DynamicConfigPublisher = _
  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      status = to
      if (to == SHUTDOWN) awaitShutdownCond.signalAll()
    } finally {
      lock.unlock()
    }
    true
  }

  private def doRemoteKraftSetup(): Unit = {
    // Explicitly configure metric reporters on this remote controller.
    // We do not yet support dynamic reconfiguration on remote controllers in general;
    // remove this once that is implemented.
    new DynamicMetricReporterState(config.nodeId, config, metrics, clusterId)
  }

  def clusterId: String = metaProperties.clusterId

  def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      info("Starting controller")

      maybeChangeStatus(STARTING, STARTED)
      this.logIdent = new LogContext(s"[ControllerServer id=${config.nodeId}] ").logPrefix()

      newGauge("ClusterId", () => clusterId)
      newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

      linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time, logger.underlying)
      if (linuxIoMetricsCollector.usable()) {
        newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
        newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
      }

      val javaListeners = config.controllerListeners.map(_.toJava).asJava
      authorizer = config.createNewAuthorizer()
      authorizer.foreach(_.configure(config.originals))

      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
        case Some(authZ) =>
          // It would be nice to remove some of the broker-specific assumptions from
          // AuthorizerServerInfo, such as the assumption that there is an inter-broker
          // listener, or that ID is named brokerId.
          val controllerAuthorizerInfo = ServerInfo(
            new ClusterResource(clusterId),
            config.nodeId,
            javaListeners,
            javaListeners.get(0),
            config.earlyStartListeners.map(_.value()).asJava)
          authZ.start(controllerAuthorizerInfo).asScala.map { case (ep, cs) =>
            ep -> cs.toCompletableFuture
          }.toMap
        case None =>
          javaListeners.asScala.map {
            ep => ep -> CompletableFuture.completedFuture[Void](null)
          }.toMap
      }

      val apiVersionManager = new SimpleApiVersionManager(ListenerType.CONTROLLER)

      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)
      socketServer = new SocketServer(config,
        metrics,
        time,
        credentialProvider,
        apiVersionManager)

      if (config.controllerListeners.nonEmpty) {
        socketServerFirstBoundPortFuture.complete(socketServer.boundPort(
          config.controllerListeners.head.listenerName))
      } else {
        throw new ConfigException("No controller.listener.names defined for controller")
      }

      val threadNamePrefixAsString = threadNamePrefix.getOrElse("")

      createTopicPolicy = Option(config.
        getConfiguredInstance(CreateTopicPolicyClassNameProp, classOf[CreateTopicPolicy]))
      alterConfigPolicy = Option(config.
        getConfiguredInstance(AlterConfigPolicyClassNameProp, classOf[AlterConfigPolicy]))

      val controllerNodes = RaftConfig.voterConnectionsToNodes(controllerQuorumVotersFuture.get())
      val quorumFeatures = QuorumFeatures.create(config.nodeId, raftApiVersions, QuorumFeatures.defaultFeatureMap(), controllerNodes)

      val controllerBuilder = {
        val leaderImbalanceCheckIntervalNs = if (config.autoLeaderRebalanceEnable) {
          OptionalLong.of(TimeUnit.NANOSECONDS.convert(config.leaderImbalanceCheckIntervalSeconds, TimeUnit.SECONDS))
        } else {
          OptionalLong.empty()
        }

        val maxIdleIntervalNs = config.metadataMaxIdleIntervalNs.fold(OptionalLong.empty)(OptionalLong.of)

        new QuorumController.Builder(config.nodeId, metaProperties.clusterId).
          setTime(time).
          setThreadNamePrefix(threadNamePrefixAsString).
          setConfigSchema(configSchema).
          setRaftClient(raftManager.client).
          setQuorumFeatures(quorumFeatures).
          setDefaultReplicationFactor(config.defaultReplicationFactor.toShort).
          setDefaultNumPartitions(config.numPartitions.intValue()).
          setSessionTimeoutNs(TimeUnit.NANOSECONDS.convert(config.brokerSessionTimeoutMs.longValue(),
            TimeUnit.MILLISECONDS)).
          setLeaderImbalanceCheckIntervalNs(leaderImbalanceCheckIntervalNs).
          setMaxIdleIntervalNs(maxIdleIntervalNs).
          setMetrics(controllerMetrics).
          setCreateTopicPolicy(createTopicPolicy.asJava).
          setAlterConfigPolicy(alterConfigPolicy.asJava).
          setConfigurationValidator(new ControllerConfigurationValidator()).
          setStaticConfig(config.originals).
          setBootstrapMetadata(bootstrapMetadata).
          setFatalFaultHandler(fatalFaultHandler)
      }
      authorizer match {
        case Some(a: ClusterMetadataAuthorizer) => controllerBuilder.setAuthorizer(a)
        case _ => // nothing to do
      }
      controller = controllerBuilder.build()

      // Perform any setup that is done only when this node is a controller-only node.
      if (!config.processRoles.contains(BrokerRole)) {
        doRemoteKraftSetup()
      }

      quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
      controllerApis = new ControllerApis(socketServer.dataPlaneRequestChannel,
        authorizer,
        quotaManagers,
        time,
        controller,
        raftManager,
        config,
        metaProperties,
        controllerNodes.asScala.toSeq,
        apiVersionManager)
      controllerApisHandlerPool = new KafkaRequestHandlerPool(config.nodeId,
        socketServer.dataPlaneRequestChannel,
        controllerApis,
        time,
        config.numIoThreads,
        s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent",
        DataPlaneAcceptor.ThreadPrefix)

      /**
       * Enable the controller endpoint(s). If we are using an authorizer which stores
       * ACLs in the metadata log, such as StandardAuthorizer, we will be able to start
       * accepting requests from principals included super.users right after this point,
       * but we will not be able to process requests from non-superusers until the
       * QuorumController declares that we have caught up to the high water mark of the
       * metadata log. See @link{QuorumController#maybeCompleteAuthorizerInitialLoad}
       * and KIP-801 for details.
       */
      socketServer.enableRequestProcessing(authorizerFutures)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during controller startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
    config.dynamicConfig.addReconfigurables(this)
    if (!config.processRoles.contains(BrokerRole)) {
      // In standalone mode, install a DynamicConfigPublisher.
      // In combined mode, we can rely on the broker's DynamicConfigPublisher.
      val dynamicConfigHandlers = immutable.Map[String, ConfigHandler](
        ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers))
      dynamicConfigPublisher = new DynamicConfigPublisher(
        config,
        fatalFaultHandler,
        dynamicConfigHandlers)
      metadataLoader.installPublishers(List(dynamicConfigPublisher).asJava)
    }
  }

  def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")
      if (socketServer != null)
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      if (controller != null) {
        controller.beginShutdown()
      }
      metadataLoader.beginShutdown()
      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)
      if (controllerApisHandlerPool != null)
        CoreUtils.swallow(controllerApisHandlerPool.shutdown(), this)
      if (controllerApis != null)
        CoreUtils.swallow(controllerApis.close(), this)
      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)
      if (controller != null)
        controller.close()
      CoreUtils.swallow(metadataLoader.close(), this)
      CoreUtils.swallow(authorizer.foreach(_.close()), this)
      createTopicPolicy.foreach(policy => CoreUtils.swallow(policy.close(), this))
      alterConfigPolicy.foreach(policy => CoreUtils.swallow(policy.close(), this))
      socketServerFirstBoundPortFuture.completeExceptionally(new RuntimeException("shutting down"))
    } catch {
      case e: Throwable =>
        fatal("Fatal error during controller shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }
}
