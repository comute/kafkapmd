/**
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

package kafka.integration

import java.util.Properties

import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}

import scala.jdk.CollectionConverters._
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}
import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaYammerMetrics

class MetricsDuringTopicCreationDeletionTest extends KafkaServerTestHarness with Logging {

  private val nodesNum = 3
  private val topicName = "topic"
  private val topicNum = 2
  private val replicationFactor = 3
  private val partitionNum = 3
  private val createDeleteIterations = 3

  private val overridingProps = new Properties
  overridingProps.put(KafkaConfig.DeleteTopicEnableProp, "true")
  overridingProps.put(KafkaConfig.AutoCreateTopicsEnableProp, "false")
  // speed up the test for UnderReplicatedPartitions, which relies on the ISR expiry thread to execute concurrently with topic creation
  // But the replica.lag.time.max.ms value still need to consider the slow Jenkins testing environment
  overridingProps.put(KafkaConfig.ReplicaLagTimeMaxMsProp, "4000")

  private val testedMetrics = List("OfflinePartitionsCount","PreferredReplicaImbalanceCount","UnderReplicatedPartitions")
  private val topics = List.tabulate(topicNum) (n => topicName + n)

  @volatile private var running = true

  override def generateConfigs = TestUtils.createBrokerConfigs(nodesNum, zkConnect)
    .map(KafkaConfig.fromProps(_, overridingProps))

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    // Do some Metrics Registry cleanup by removing the metrics that this test checks.
    // This is a test workaround to the issue that prior harness runs may have left a populated registry.
    // see https://issues.apache.org/jira/browse/KAFKA-4605
    for (m <- testedMetrics) {
        val metricName = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.keys.find(_.getName.endsWith(m))
        metricName.foreach(KafkaYammerMetrics.defaultRegistry.removeMetric)
    }

    super.setUp(testInfo)
  }

  /*
   * checking all metrics we care in a single test is faster though it would be more elegant to have 3 @Test methods
   */
  @Test
  def testMetricsDuringTopicCreateDelete(): Unit = {

    // For UnderReplicatedPartitions, because of https://issues.apache.org/jira/browse/KAFKA-4605
    // we can't access the metrics value of each server. So instead we directly invoke the method
    // replicaManager.underReplicatedPartitionCount() that defines the metrics value.
    @volatile var underReplicatedPartitionCount = 0

    // For OfflinePartitionsCount and PreferredReplicaImbalanceCount even with https://issues.apache.org/jira/browse/KAFKA-4605
    // the test has worked reliably because the metric that gets triggered is the one generated by the first started server (controller)
    val offlinePartitionsCountGauge = getGauge("OfflinePartitionsCount")
    @volatile var offlinePartitionsCount = offlinePartitionsCountGauge.value
    assert(offlinePartitionsCount == 0)

    val preferredReplicaImbalanceCountGauge = getGauge("PreferredReplicaImbalanceCount")
    @volatile var preferredReplicaImbalanceCount = preferredReplicaImbalanceCountGauge.value
    assert(preferredReplicaImbalanceCount == 0)

    // Thread checking the metric continuously
    running = true
    val thread = new Thread(() => {
      while (running) {
        for (s <- servers if running) {
          underReplicatedPartitionCount = s.replicaManager.underReplicatedPartitionCount
          if (underReplicatedPartitionCount > 0) {
            running = false
          }
        }

        preferredReplicaImbalanceCount = preferredReplicaImbalanceCountGauge.value
        if (preferredReplicaImbalanceCount > 0) {
          running = false
        }

        offlinePartitionsCount = offlinePartitionsCountGauge.value
        if (offlinePartitionsCount > 0) {
          running = false
        }
      }
    })
    thread.start

    // breakable loop that creates and deletes topics
    createDeleteTopics()

    // if the thread checking the gauge is still run, stop it
    running = false;
    thread.join

    assert(offlinePartitionsCount==0, s"Expect offlinePartitionsCount to be 0, but got: $offlinePartitionsCount")
    assert(preferredReplicaImbalanceCount==0, s"Expect PreferredReplicaImbalanceCount to be 0, but got: $preferredReplicaImbalanceCount")
    assert(underReplicatedPartitionCount==0, s"Expect UnderReplicatedPartitionCount to be 0, but got: $underReplicatedPartitionCount")
  }

  private def getGauge(metricName: String) = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (k, _) => k.getName.endsWith(metricName) }
      .getOrElse(throw new AssertionError( "Unable to find metric " + metricName))
      ._2.asInstanceOf[Gauge[Int]]
  }

  private def createDeleteTopics(): Unit = {
    for (l <- 1 to createDeleteIterations if running) {
      // Create topics
      for (t <- topics if running) {
        try {
          createTopic(t, partitionNum, replicationFactor)
        } catch {
          case e: Exception => e.printStackTrace
        }
      }

      // Delete topics
      for (t <- topics if running) {
          try {
            adminZkClient.deleteTopic(t)
            TestUtils.verifyTopicDeletion(zkClient, t, partitionNum, servers)
          } catch {
          case e: Exception => e.printStackTrace
          }
      }
    }
  }
}
