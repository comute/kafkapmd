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
package kafka.api

import java.io.File
import java.util.Collections
import java.util.concurrent.{ExecutionException, TimeUnit}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.junit.{Before, Test}
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue

/**
  * Test whether clients can producer and consume when there is log directory failure
  */
class LogDirFailureTest extends IntegrationTestHarness {
  val producerCount: Int = 1
  val consumerCount: Int = 1
  val serverCount: Int = 2
  private val topic = "topic"

  this.logDirCount = 2
  this.producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "0")
  this.producerConfig.setProperty(ProducerConfig.METADATA_MAX_AGE_CONFIG, "100")
  this.serverConfig.setProperty(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp, "100")

  @Before
  override def setUp() {
    super.setUp()
    TestUtils.createTopic(zkUtils, topic, 1, 2, servers = servers)
  }

  @Test
  def testProduceAfterLogDirFailure() {
    val consumer = consumers.head
    subscribeAndWaitForAssignment(topic, consumer)
    val producer = producers.head
    val partition = new TopicPartition(topic, 0)
    val record = new ProducerRecord(topic, 0, s"key".getBytes, s"value".getBytes)

    val leaderServer =
      if (servers.head.replicaManager.getPartition(partition).flatMap(_.leaderReplicaIfLocal).isDefined) servers.head
      else servers.tail.head

    // The first send() should succeed
    producer.send(record).get()
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0).count() == 1
    }, "Expected the first message", 3000L)

    // Make log directory of the partition on the leader broker inaccessible by replacing it with a file
    val replica = leaderServer.replicaManager.getReplica(partition)
    val logDir = replica.get.log.get.dir.getParentFile
    deleteRecursively(logDir)
    logDir.createNewFile()
    assertTrue(logDir.isFile)

    // Wait for ReplicaHighWatermarkCheckpoint to happen so that the log directory of the topic will be offline
    TestUtils.waitUntilTrue(() => !leaderServer.logManager.liveLogDirs.contains(logDir), "Expected log directory offline", 3000L)
    assertTrue(leaderServer.replicaManager.getReplica(partition).isEmpty)

    // The second send() should fail due to UnknownTopicOrPartitionException
    try {
      producer.send(record).get(6000, TimeUnit.MILLISECONDS)
      fail("send() should fail with UnknownTopicOrPartitionException")
    } catch {
      case e: ExecutionException => assertEquals(classOf[UnknownTopicOrPartitionException], e.getCause.getClass)
      case e: Throwable => fail(s"send() should fail with UnknownTopicOrPartitionException instead of ${e.toString}")
    }

    // Wait for producer to update metadata for the partition
    TestUtils.waitUntilTrue(() => {
      producer.partitionsFor(topic).get(0).leader().id() != leaderServer.config.brokerId
    }, "Expected new leader for the partition", 3000L)

    // The third send() should succeed
    producer.send(record).get()
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0).count() == 1
    }, "Expected the second message", 3000L)
  }

  private def subscribeAndWaitForAssignment(topic: String, consumer: KafkaConsumer[Array[Byte], Array[Byte]]) {
    consumer.subscribe(Collections.singletonList(topic))
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0)
      !consumer.assignment.isEmpty
    }, "Expected non-empty assignment")
  }

  private def deleteRecursively(file: File) {
    val files = file.listFiles()
    if (files != null) {
      files.foreach(f => deleteRecursively(f))
    }
    file.delete()
  }

}
