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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.Seq

import kafka.server.Constants._
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.metrics._

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.stats.SimpleRate
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.config.ReplicationQuotaManagerConfig

trait ReplicaQuota {
  def record(value: Long): Unit
  def isThrottled(topicPartition: TopicPartition): Boolean
  def isQuotaExceeded: Boolean
}

object Constants {
  val AllReplicas = Seq[Int](-1)
}

/**
  * Tracks replication metrics and comparing them to any quotas for throttled partitions.
  *
  * @param config          The quota configs
  * @param metrics         The Metrics instance
  * @param replicationType The name / key for this quota manager, typically leader or follower
  * @param time            Time object to use
  */
class ReplicationQuotaManager(val config: ReplicationQuotaManagerConfig,
                              private val metrics: Metrics,
                              private val replicationType: QuotaType,
                              private val time: Time) extends Logging with ReplicaQuota {
  private val lock = new ReentrantReadWriteLock()
  private val throttledPartitions = new ConcurrentHashMap[String, Seq[Int]]()
  private var quota: Quota = _
  private val sensorAccess = new SensorAccess(lock, metrics)
  private val rateMetricName = metrics.metricName("byte-rate", replicationType.toString,
    s"Tracking byte-rate for $replicationType")

  /**
    * Update the quota of metrics instance
    *
    * @param quota         The quota of metrics we want to update.
    */
  def updateQuota(quota: Quota): Unit = {
    inWriteLock(lock) {
      this.quota = quota
      //The metric could be expired by another thread, so use a local variable and null check.
      val metric = metrics.metrics.get(rateMetricName)
      if (metric != null) {
        metric.config(getQuotaMetricConfig(quota))
      }
    }
  }

  /**
    * Check if the quota is currently exceeded
    *
    * @return false : if the quota is currently exceeded.
   *          true  : if we violated our quota for any metric that has a configured quota
    */
  override def isQuotaExceeded: Boolean = {
    try {
      sensor().checkQuotas()
    } catch {
      case qve: QuotaViolationException =>
        trace(s"$replicationType: Quota violated for sensor (${sensor().name}), metric: (${qve.metric.metricName}), " +
          s"metric-value: (${qve.value}), bound: (${qve.bound})")
        return true
    }
    false
  }

  /**
    * Is the passed partition throttled by this ReplicationQuotaManager
    *
    * @param topicPartition the partition to check
    * @return false if the partition doesn't exist otherwise return false
    */
  override def isThrottled(topicPartition: TopicPartition): Boolean = {
    val partitions = throttledPartitions.get(topicPartition.topic)
    if (partitions != null)
      (partitions eq AllReplicas) || partitions.contains(topicPartition.partition)
    else false
  }

  /**
    * Add the passed value to the throttled rate. This method ignores the quota with
    * the value being added to the rate even if the quota is exceeded
    *
    * @param value a assigned value that we want to record
    */
  def record(value: Long): Unit = {
    sensor().record(value.toDouble, time.milliseconds(), false)
  }

  /**
    * Update the set of throttled partitions for this QuotaManager. The partitions passed, for
    * any single topic, will replace any previous
    *
    * @param topic the target topic to mark
    * @param partitions the set of throttled partitions
    */
  def markThrottled(topic: String, partitions: Seq[Int]): Unit = {
    throttledPartitions.put(topic, partitions)
  }

  /**
    * Mark all replicas for this topic as throttled
    *
    * @param topic the target topic to mark.
    *
    */
  def markThrottled(topic: String): Unit = {
    markThrottled(topic, AllReplicas)
  }

  /**
    * Remove list of throttled replicas for a certain topic
    *
    * @param topic the target topic to remove.
    * @return the previous partitions associated with key, or null if there was no mapping for key.
    */
  def removeThrottle(topic: String): Unit = {
    throttledPartitions.remove(topic)
  }

  /**
    * Returns the bound of the configured quota
    *
    * @return bounded value if the configured quota is defined otherwise return the max value of long.
    *
    */
  def upperBound: Long = {
    inReadLock(lock) {
      if (quota != null)
        quota.bound.toLong
      else
        Long.MaxValue
    }
  }

  /**
   * get the bound of the configured quota
   *
   * @return new configuration value of quota metric.
   */

  private def getQuotaMetricConfig(quota: Quota): MetricConfig = {
    new MetricConfig()
      .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
      .samples(config.numQuotaSamples)
      .quota(quota)
  }

  private def sensor(): Sensor = {
    sensorAccess.getOrCreate(
      replicationType.toString,
      ReplicationQuotaManagerConfig.INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS,
      sensor => sensor.add(rateMetricName, new SimpleRate, getQuotaMetricConfig(quota))
    )
  }
}
