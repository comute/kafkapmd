/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Partition
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{CachedConfigRepository, MetadataBroker, MetadataBrokers, MetadataImage, MetadataImageBuilder, MetadataPartition, RaftMetadataCache}
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.metadata.PartitionRecord
import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, never, verify, when}
import org.slf4j.Logger

import scala.collection.mutable

trait LeadershipChangeHandler {
  def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]): Unit
}

class RaftReplicaManagerTest {
  private var alterIsrManager: AlterIsrManager = _
  private var config: KafkaConfig = _
  private val configRepository = new CachedConfigRepository()
  private val metrics = new Metrics
  private var quotaManager: QuotaManagers = _
  private val time = new MockTime
  private var mockDelegate: RaftReplicaChangeDelegate = _
  private var imageBuilder: MetadataImageBuilder = _
  private val brokerId0 = 0
  private val metadataBroker0 = new MetadataBroker(brokerId0, null, Map.empty, false)
  private val brokerId1 = 1
  private val metadataBroker1 = new MetadataBroker(brokerId1, null, Map.empty, false)
  private val topicName = "topicName"
  private val topicId = Uuid.randomUuid()
  private val partitionId0 = 0
  private val partitionId1 = 1
  private val topicPartition0 = new TopicPartition(topicName, partitionId0)
  private val topicPartition1 = new TopicPartition(topicName, partitionId1)
  private val topicPartitionRecord0 = new PartitionRecord()
    .setPartitionId(partitionId0)
    .setTopicId(topicId)
    .setReplicas(util.Arrays.asList(brokerId0, brokerId1))
    .setLeader(brokerId0)
    .setLeaderEpoch(0)
  private val topicPartitionRecord1 = new PartitionRecord()
    .setPartitionId(partitionId1)
    .setTopicId(topicId)
    .setReplicas(util.Arrays.asList(brokerId0, brokerId1))
    .setLeader(brokerId1)
    .setLeaderEpoch(0)
  private val offset1 = 1L
  private val metadataPartition0Deferred = MetadataPartition(topicName, topicPartitionRecord0, Some(offset1))
  private val metadataPartition1Deferred = MetadataPartition(topicName, topicPartitionRecord1, Some(offset1))
  private val metadataPartition0NoLongerDeferred = MetadataPartition(metadataPartition0Deferred)
  private val metadataPartition1NoLongerDeferred = MetadataPartition(metadataPartition1Deferred)
  private val onLeadershipChangeHandler = mock(classOf[LeadershipChangeHandler])
  private val onLeadershipChange = onLeadershipChangeHandler.onLeadershipChange _
  private var metadataCache: RaftMetadataCache = _

  @BeforeEach
  def setUp(): Unit = {
    alterIsrManager = mock(classOf[AlterIsrManager])
    config = KafkaConfig.fromProps({
      val nodeId = brokerId0
      val props = TestUtils.createBrokerConfig(nodeId, "")
      props.put(KafkaConfig.ProcessRolesProp, "broker")
      props.put(KafkaConfig.NodeIdProp, nodeId.toString)
      props
    })
    metadataCache = new RaftMetadataCache(config.brokerId)
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "")
    mockDelegate = mock(classOf[RaftReplicaChangeDelegate])
    imageBuilder = MetadataImageBuilder(brokerId0, mock(classOf[Logger]), new MetadataImage())
  }

  @AfterEach
  def tearDown(): Unit = {
    TestUtils.clearYammerMetrics()
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  def createRaftReplicaManager(): RaftReplicaManager = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    new RaftReplicaManager(config, metrics, time, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
      metadataCache, new LogDirFailureChannel(config.logDirs.size), alterIsrManager,
      configRepository, None)
  }

  @Test
  def testRejectsZkConfig(): Unit = {
    assertThrows(classOf[IllegalStateException], () => {
      val zkConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, ""))
      val mockLogMgr = TestUtils.createLogManager(zkConfig.logDirs.map(new File(_)))
      new RaftReplicaManager(zkConfig, metrics, time, new MockScheduler(time), mockLogMgr,
        new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
        metadataCache, new LogDirFailureChannel(config.logDirs.size), alterIsrManager,
        configRepository)
    })
  }

  @Test
  def testDefersChangesImmediatelyThenAppliesChanges(): Unit = {
    val rrm = createRaftReplicaManager()
    rrm.delegate = mockDelegate

    processTopicPartitionMetadata(rrm)
    // verify changes would have been deferred
    val partitionsNewMapCaptor: ArgumentCaptor[mutable.Map[Partition, Boolean]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, Boolean]])
    verify(mockDelegate).makeDeferred(partitionsNewMapCaptor.capture(), ArgumentMatchers.eq(offset1), ArgumentMatchers.eq(onLeadershipChange))
    val partitionsDeferredMap = partitionsNewMapCaptor.getValue
    assertEquals(2, partitionsDeferredMap.size)
    val partition0 = partitionsDeferredMap.keys.filter(partition => partition.topicPartition == topicPartition0).head
    assertTrue(partitionsDeferredMap(partition0))
    val partition1 = partitionsDeferredMap.keys.filter(partition => partition.topicPartition == topicPartition1).head
    assertTrue(partitionsDeferredMap(partition1))
    verify(mockDelegate, never()).makeLeaders(any(), any(), any(), anyLong())
    verify(mockDelegate, never()).makeFollowers(any(), any(), any(), any(), any())

    // now mark those topic partitions as being deferred so we can later apply the changes
    rrm.markPartitionDeferred(partition0, isNew = true, onLeadershipChange)
    rrm.markPartitionDeferred(partition1, isNew = true, onLeadershipChange)

    // apply the changes
    // first update the partitions in the metadata cache so they aren't marked as being deferred
    val imageBuilder2 = MetadataImageBuilder(brokerId0, mock(classOf[Logger]), metadataCache.currentImage())
    imageBuilder2.partitionsBuilder().set(metadataPartition0NoLongerDeferred)
    imageBuilder2.partitionsBuilder().set(metadataPartition1NoLongerDeferred)
    metadataCache.image(imageBuilder2.build())
    // define some return values to avoid NPE
    when(mockDelegate.makeLeaders(any(), any(), any(), anyLong())).thenReturn(Set(partition0))
    when(mockDelegate.makeFollowers(any(), any(), any(), any(), anyLong())).thenReturn(Set(partition1))
    rrm.endMetadataChangeDeferral()
    // verify that the deferred changes would have been applied
    // leaders...
    val leaderPartitionStatesCaptor: ArgumentCaptor[mutable.Map[Partition, MetadataPartition]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, MetadataPartition]])
    verify(mockDelegate).makeLeaders(ArgumentMatchers.eq(mutable.Set()), leaderPartitionStatesCaptor.capture(), any(),
      ArgumentMatchers.eq(MetadataPartition.OffsetNeverDeferred))
    val leaderPartitionStates = leaderPartitionStatesCaptor.getValue
    assertEquals(Map(partition0 -> metadataPartition0NoLongerDeferred), leaderPartitionStates)
    // followers...
    val followerPartitionStatesCaptor: ArgumentCaptor[mutable.Map[Partition, MetadataPartition]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, MetadataPartition]])
    val brokersCaptor: ArgumentCaptor[MetadataBrokers] = ArgumentCaptor.forClass(classOf[MetadataBrokers])
    verify(mockDelegate).makeFollowers(ArgumentMatchers.eq(mutable.Set()), brokersCaptor.capture(),
      followerPartitionStatesCaptor.capture(), any(), ArgumentMatchers.eq(MetadataPartition.OffsetNeverDeferred))
    val followerPartitionStates = followerPartitionStatesCaptor.getValue
    assertEquals(Map(partition1 -> metadataPartition1NoLongerDeferred), followerPartitionStates)
    val brokers = brokersCaptor.getValue
    assertEquals(2, brokers.size())
    assertTrue(brokers.aliveBroker(brokerId0).isDefined)
    assertTrue(brokers.aliveBroker(brokerId1).isDefined)
    // verify leadership change callbacks is invoked correctly
    val updatedLeaders: ArgumentCaptor[Iterable[Partition]] = ArgumentCaptor.forClass(classOf[Iterable[Partition]])
    val updatedFollowers: ArgumentCaptor[Iterable[Partition]] = ArgumentCaptor.forClass(classOf[Iterable[Partition]])
    verify(onLeadershipChangeHandler).onLeadershipChange(updatedLeaders.capture(), updatedFollowers.capture())
    assertEquals(List(partition0), updatedLeaders.getValue.toList)
    assertEquals(List(partition1), updatedFollowers.getValue.toList)
  }

  private def processTopicPartitionMetadata(raftReplicaManager: RaftReplicaManager): Unit = {
    // create brokers
    imageBuilder.brokersBuilder().add(metadataBroker0)
    imageBuilder.brokersBuilder().add(metadataBroker1)
    // create topic
    imageBuilder.partitionsBuilder().addUuidMapping(topicName, topicId)
    // create deferred partitions
    imageBuilder.partitionsBuilder().set(metadataPartition0Deferred)
    imageBuilder.partitionsBuilder().set(metadataPartition1Deferred)
    // apply the changes to metadata cache
    metadataCache.image(imageBuilder.build())
    // apply the changes to replica manager
    raftReplicaManager.handleMetadataRecords(imageBuilder, offset1, onLeadershipChange)
  }
}
