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

package unit.kafka.server

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.admin.AdminUtils._
import kafka.common._
import kafka.log.LogConfig._
import kafka.server.KafkaConfig.fromProps
import kafka.server.QuotaType.{LeaderReplication, FollowerReplication}
import kafka.server._
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.MetricName
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class ReplicationQuotasTest extends ZooKeeperTestHarness {
  def tenPercentError(value: Int): Int = Math.round(value * 0.1).toInt

  def fifteenPercentError(value: Int): Int = Math.round(value * 0.15).toInt

  val msg100KB = new Array[Byte](100000)
  var brokers: Seq[KafkaServer] = null
  val topic = "topic1"
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null

  @Before
  override def setUp() {
    super.setUp()
  }

  @After
  override def tearDown() {
    brokers.par.foreach(_.shutdown())
    producer.close()
    super.tearDown()
  }

  @Test //make this test faster by reducing the window lengths
  def shouldThrottleToDesiredRateOnLeaderOverTime(): Unit = {
    brokers = createBrokerConfigs(2, zkConnect).map(fromProps).map(TestUtils.createServer(_))
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5, acks = 0)
    val leaders = TestUtils.createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 2, servers = brokers)
    val leader = if (leaders(0).get == brokers.head.config.brokerId) brokers.head else brokers(1)
    val leaderByteRateMetricName = leader.metrics.metricName("byte-rate", LeaderReplication, "Tracking byte-rate for " + LeaderReplication)

    shouldThrottleToDesiredRateOverTime(leader,  leaderByteRateMetricName)
  }

  @Test //make this test faster by reducing the window lengths
  def shouldThrottleToDesiredRateOFollowerOverTime(): Unit = {
    brokers = createBrokerConfigs(2, zkConnect).map(fromProps).map(TestUtils.createServer(_))
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5, acks = 0)
    val leaders = TestUtils.createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 2, servers = brokers)
    val follower = if (leaders(0).get == brokers.head.config.brokerId) brokers(1) else brokers.head
    val followerByteRateMetricName = follower.metrics.metricName("byte-rate", FollowerReplication, "Tracking byte-rate for" + FollowerReplication)

    shouldThrottleToDesiredRateOverTime(follower, followerByteRateMetricName)
  }

  //TODO need to work on the temporal comparisons prior to merge
  def shouldThrottleToDesiredRateOverTime(brokerUnderTest: KafkaServer, metricName: MetricName) {

    /**
      * This test will fail if the rate is < 1MB/s as 1MB is replica.fetch.max.bytes.
      * So with a throttle of 100KB/s 1 fetch of 1 partition would fill 10s of quota.
      * In doing so sending the quota way over, then blocking for ten seconds. Thus
      * it is likely the test will fail, depending on when in the cycle it completes.
      *
      * Keep the throttle value > 1MB/s to get stability.
      *
      */

    //Given
    val msg = msg100KB
    val throttle: Int = 10 * msg.length
    val msgCount: Int = 100

    //Propagate throttle value and list of throttled partitions
    changeBrokerConfig(zkUtils, (0 until brokers.length), property(KafkaConfig.ThrottledReplicationRateLimitProp, throttle.toString))
    changeTopicConfig(zkUtils, topic, property(ThrottledReplicasListProp, "*"))

    val start = System.currentTimeMillis()

    //When we load with data (acks = 0)
    for (x <- 0 until msgCount)
      producer.send(new ProducerRecord(topic, msg))

    //Wait for replication to complete
    def logsMatchAtOffset() = waitForOffset(tp(topic, 0), msgCount)
    waitUntilTrue(logsMatchAtOffset, "Broker logs should be identical and have offset " + msgCount, 100000)
    val took = System.currentTimeMillis() - start

    //Then the recorded rate should match the quota we defined
    val throttledRateFromLeader = brokerUnderTest.metrics.metrics.asScala(metricName).value()
    info(s"Expected:$throttle, Recorded Rate was:$throttledRateFromLeader")
    assertEquals(throttle, throttledRateFromLeader, tenPercentError(throttle))

    //Then also check it took the expected amount of time (don't merge this as is)
    val expectedDuration = msgCount / (throttle / msg.length) * 1000
    info(s"Expected:$expectedDuration, Took:$took")
    assertEquals(expectedDuration, took, expectedDuration * 0.2)
  }

  @Test
  def shouldReplicateThrottledAndNonThrottledPartitionsConcurrentlyWhenLeaderThrottleEngaged() {
    shouldReplicateThrottledAndNonThrottledPartitionsConcurrently(LeaderReplication)
  }

  @Test
  def shouldReplicateThrottledAndNonThrottledPartitionsConcurrentlyWhenFollowerThrottleEngaged() {
    shouldReplicateThrottledAndNonThrottledPartitionsConcurrently(FollowerReplication)
  }

  //TODO need to work on the temporal comparisons prior to merge
  def shouldReplicateThrottledAndNonThrottledPartitionsConcurrently(throttleSide: String): Unit = {
    brokers = createBrokerConfigs(2, zkConnect).map(fromProps).map(TestUtils.createServer(_))
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5, acks = 0)

    //Given 4 partitions, all lead on server 0, we'll throttle two of them
    TestUtils.createTopic(zkUtils, topic, Map(0 -> Seq(0, 1), 1 -> Seq(0, 1), 2 -> Seq(0, 1), 3 -> Seq(0, 1)), brokers)

    //Define test settings
    val msg = msg100KB
    val throttle: Int = 10 * msg.length
    val msgCount: Int = 50

    //Set the throttle config and replicas list so partition 0 & 2, only, are throttled
    changeBrokerConfig(zkUtils, (0 until brokers.length), property(KafkaConfig.ThrottledReplicationRateLimitProp, throttle.toString))
    val side: Int = if (throttleSide == FollowerReplication) 1 else 0
    changeTopicConfig(zkUtils, topic, property(ThrottledReplicasListProp, s"0-$side:2-$side")) //partition 0 & 2 are throttled

    //Useful functions
    def configPropagatedCondition(): Boolean = brokers(side).quotaManagers.leader.isThrottled(new TopicAndPartition(topic, 0)) //doesn't matter if we check leader or follower replication
    def logsMatchRegular() = waitForOffset(tp(topic, 1), msgCount) && waitForOffset(tp(topic, 3), msgCount)
    def logsMatchThrottled() = waitForOffset(tp(topic, 0), msgCount) && waitForOffset(tp(topic, 2), msgCount) //the throttled ones

    //Wait for config to propagate
    waitUntilTrue(configPropagatedCondition, "Throttled partition config should have propagated.")

    val start: Long = System.currentTimeMillis()

    //Write a message to each partition individually to get an even spread
    for (x <- 0 until msgCount)
      for (partition <- (0 to 3))
        producer.send(new ProducerRecord(topic, partition, null, msg))

    waitUntilTrue(logsMatchRegular, "Partition 1 or 3's logs didn't match", 30000)

    var took = System.currentTimeMillis() - start
    assertTrue("Partition 1 & 3 should have replicated quickly: " + took, took < 2000)

    waitUntilTrue(logsMatchThrottled, "Throttled partitions (0,2) logs didn't match")

    val expectedDuration = msgCount / (throttle / msg.length) * 1000 * 2 // i.e 2 throttled partitions
    took = System.currentTimeMillis() - start
    info(s"expected: $expectedDuration, was: $took")
    assertEquals(s"Throttled partitions should have been slow. Was $took ms", expectedDuration, took, fifteenPercentError(expectedDuration))
  }

  @Test
  def shouldMatchQuotaReplicatingFromSixServersToTwoWithThrottleOnTheSixLeaders(): Unit = {
    shouldMatchQuotaReplicatingAnAsymetricTopology(true)
  }

  @Test
  def shouldMatchQuotaReplicatingFromSixServersToTwoWithThrottleOnTheTwoFollowers(): Unit = {
    shouldMatchQuotaReplicatingAnAsymetricTopology(false)
  }

  def shouldMatchQuotaReplicatingAnAsymetricTopology(leaderThrottle: Boolean): Unit = {
    brokers = createBrokerConfigs(6, zkConnect).map(fromProps).map(TestUtils.createServer(_))

    //Given three partitions, lead on nodes 0,1,2,3,4,5 but will followers on node 6,7 (not started yet)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic,
      Map(0 -> Seq(0, 6), 1 -> Seq(1, 6), 2 -> Seq(2, 6), 3 -> Seq(3, 7), 4 -> Seq(4, 7), 5 -> Seq(5, 7)))

    val msg = msg100KB
    val msgCount: Int = 100
    val expectedDuration = 5 //Keep the test to N seconds
    var throttle: Int = msgCount * msg.length / expectedDuration
    if (!leaderThrottle) throttle = throttle * 3 //Follower throttle needs to replicate 3x as fast to get the same duration as there are three replicas to replicate for each of the two follower brokers

    //Set the throttle on either the three leaders or the one follower
    (0 to 7).foreach { brokerId =>
      changeBrokerConfig(zkUtils, Seq(brokerId), property(KafkaConfig.ThrottledReplicationRateLimitProp, throttle.toString))
    }
    if(leaderThrottle)
      changeTopicConfig(zkUtils, topic, property(ThrottledReplicasListProp, "0-0:1-1:2-2:3-3:4-4:5-5"))//partition-broker:...
    else
      changeTopicConfig(zkUtils, topic, property(ThrottledReplicasListProp, "0-6:1-6:2-6:3-7:4-7:5-7"))//partition-broker:...

    //Add data
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5, acks = 0)
    (0 until msgCount).foreach { x =>
      (0 to 5).foreach { partition =>
        producer.send(new ProducerRecord(topic, partition, null, msg)).get
      }
    }

    //Ensure data is fully written: broker 1 has partition 1, broker 2 has partition 2 etc
    (0 to 5).foreach { partitionOrBrokerId =>
      waitUntilTrue(() => waitForOffset(TopicAndPartition(topic, partitionOrBrokerId), msgCount, Seq(brokers(partitionOrBrokerId))), "Logs didn't match for partition ", 40000)
    }

    val start = System.currentTimeMillis()

    //When we create a 4th, empty broker
    val configs = createBrokerConfigs(8, zkConnect).map(fromProps)
    brokers = brokers :+ TestUtils.createServer(configs(6))
    brokers = brokers :+ TestUtils.createServer(configs(7))

    //Wait for replicas 0,1,2,3,4,5 to fully replicated to broker 6,7
    (0 to 2).foreach { partition =>
      waitUntilTrue(() => waitForOffset(TopicAndPartition(topic, partition), msgCount, Seq(brokers(6))), "Logs didn't match for partition ", 40000)
    }
    (3 to 5).foreach { partition =>
      waitUntilTrue(() => waitForOffset(TopicAndPartition(topic, partition), msgCount, Seq(brokers(7))), "Logs didn't match for partition ", 40000)
    }
    val took = System.currentTimeMillis() - start

    //Then the throttle should slow replication beyond the expected duration.
    val message = (s"Replication took to $took but was expected to take $expectedDuration")
    assertTrue(message, took > expectedDuration * 1000)
    assertTrue(message, took < expectedDuration * 1000 * 1.5)
  }

  def tp(topic: String, partition: Int): TopicAndPartition = new TopicAndPartition(topic, partition)

  def logsMatch(): Boolean = logsMatch(TopicAndPartition(topic, 0))

  def logsMatch(topicAndPart: TopicAndPartition): Boolean = {
    var result = true
    val expectedOffset = brokers.head.getLogManager().getLog(topicAndPart).get.logEndOffset
    result = result && expectedOffset > 0 && brokers.forall { item =>
      expectedOffset == item.getLogManager().getLog(topicAndPart).get.logEndOffset
    }
    if (result) info("final offset was " + expectedOffset + " for partition " + topicAndPart)
    result
  }

  def waitForOffset(topicAndPart: TopicAndPartition, offset: Int, servers: Seq[KafkaServer] = brokers): Boolean = {
    var result = true
    result = result && servers.forall { item =>
      offset == (if (item.getLogManager().getLog(topicAndPart) == None) 0 else item.getLogManager().getLog(topicAndPart).get.logEndOffset)
    }
    result
  }

  def property(key: String, value: String) = {
    val props = new Properties()
    props.put(key, value)
    props
  }
}
