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
package kafka.server.epoch

import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.server.LogOffsetMetadata
import kafka.server.checkpoints.LeaderEpochCheckpoint
import org.apache.kafka.common.requests.EpochEndOffset._
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import scala.collection.mutable.ListBuffer

trait LeaderEpochCache {
  def assign(leaderEpoch: Int, offset: Long)
  def latestEpoch: Option[Int]
  def endOffsetFor(epoch: Int): (Int, Long)
  def clearAndFlushLatest(offset: Long)
  def clearAndFlushEarliest(offset: Long)
  def clearAndFlush()
  def clear()
}

/**
  * Represents a cache of (LeaderEpoch => Offset) mappings for a particular replica.
  *
  * Leader Epoch = epoch assigned to each leader by the controller.
  * Offset = offset of the first message in each epoch.
  *
  * @param leo a function that determines the log end offset
  * @param checkpoint the checkpoint file
  */
class LeaderEpochFileCache(topicPartition: TopicPartition, leo: () => LogOffsetMetadata, checkpoint: LeaderEpochCheckpoint) extends LeaderEpochCache with Logging {
  private val lock = new ReentrantReadWriteLock()
  private var epochs: ListBuffer[EpochEntry] = inWriteLock(lock) { ListBuffer(checkpoint.read(): _*) }

  /**
    * Assigns the supplied Leader Epoch to the supplied Offset
    * Once the epoch is assigned it cannot be reassigned
    *
    * @param epoch
    * @param offset
    */
  override def assign(epoch: Int, offset: Long): Unit = {
    inWriteLock(lock) {
      if (epoch >= 0 && epoch > latestEpoch.getOrElse(UNDEFINED_EPOCH) && offset >= latestOffset) {
        info(s"Updated PartitionLeaderEpoch. ${epochChangeMsg(epoch, offset)}. Cache now contains ${epochs.size} entries.")
        epochs += EpochEntry(epoch, offset)
        flush()
      } else {
        validateAndMaybeWarn(epoch, offset)
      }
    }
  }

  /**
    * Returns the current Leader Epoch. This is the latest epoch
    * which has messages assigned to it.
    *
    * @return
    */
  override def latestEpoch: Option[Int] = {
    inReadLock(lock) {
      if (epochs.isEmpty) None else Some(epochs.last.epoch)
    }
  }

  /**
    * Returns the Leader Epoch and the End Offset for a requested Leader Epoch.
    *
    * The Leader Epoch returned is the largest epoch less than or equal to the requested Leader
    * Epoch. The End Offset is the end offset of this epoch, which is defined as the start offset
    * of the first Leader Epoch larger than the Leader Epoch requested, or else the Log End
    * Offset if the latest epoch was requested.
    *
    * During the upgrade phase, where there are existing messages may not have a leader epoch,
    * if requestedEpoch is < the first epoch cached, UNSUPPORTED_EPOCH_OFFSET will be returned
    * so that the follower falls back to High Water Mark.
    *
    * @param requestedEpoch requested leader epoch. Must be non-negative
    * @return leader epoch and offset
    */
  override def endOffsetFor(requestedEpoch: Int): (Int, Long) = {
    inReadLock(lock) {
      val epochAndOffset =
        if (requestedEpoch == latestEpoch.getOrElse(UNDEFINED_EPOCH)) {
          (requestedEpoch, leo().messageOffset)
        } else {
          val (subsequentEpochs, previousEpochs) = epochs.partition { e => e.epoch > requestedEpoch}
          if (subsequentEpochs.isEmpty || requestedEpoch < epochs.head.epoch)
            // no epochs recorded or requested epoch < the first epoch cached
            (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
          else {
            // we must get at least one element in previous epochs list, because if we are here,
            // it means that requestedEpoch >= epochs.head.epoch -- so at least the first epoch is
            (previousEpochs.last.epoch, subsequentEpochs.head.startOffset)
          }
        }
      debug(s"Processed offset for epoch request for partition ${topicPartition} epoch:$requestedEpoch and returning epoch ${epochAndOffset._1} and offset ${epochAndOffset._2} from epoch list of size ${epochs.size}")
      epochAndOffset
    }
  }

  /**
    * Removes all epoch entries from the store with start offsets greater than or equal to the passed offset.
    *
    * @param offset
    */
  override def clearAndFlushLatest(offset: Long): Unit = {
    inWriteLock(lock) {
      val before = epochs
      if (offset >= 0 && offset <= latestOffset()) {
        epochs = epochs.filter(entry => entry.startOffset < offset)
        flush()
        info(s"Cleared latest ${before.toSet.filterNot(epochs.toSet)} entries from epoch cache based on passed offset $offset leaving ${epochs.size} in EpochFile for partition $topicPartition")
      }
    }
  }

  /**
    * Clears old epoch entries. This method searches for the oldest epoch < offset, updates the saved epoch offset to
    * be offset, then clears any previous epoch entries.
    *
    * This method is exclusive: so clearEarliest(6) will retain an entry at offset 6.
    *
    * @param offset the offset to clear up to
    */
  override def clearAndFlushEarliest(offset: Long): Unit = {
    inWriteLock(lock) {
      val before = epochs
      if (offset >= 0 && earliestOffset() < offset) {
        val earliest = epochs.filter(entry => entry.startOffset < offset)
        if (earliest.nonEmpty) {
          epochs = epochs --= earliest
          //If the offset is less than the earliest offset remaining, add previous epoch back, but with an updated offset
          if (offset < earliestOffset() || epochs.isEmpty)
            new EpochEntry(earliest.last.epoch, offset) +=: epochs
          flush()
          info(s"Cleared earliest ${before.toSet.filterNot(epochs.toSet).size} entries from epoch cache based on passed offset $offset leaving ${epochs.size} in EpochFile for partition $topicPartition")
        }
      }
    }
  }

  /**
    * Delete all entries.
    */
  override def clearAndFlush() = {
    inWriteLock(lock) {
      epochs.clear()
      flush()
    }
  }

  override def clear() = {
    inWriteLock(lock) {
      epochs.clear()
    }
  }

  def epochEntries(): ListBuffer[EpochEntry] = {
    epochs
  }

  private def earliestOffset(): Long = {
    if (epochs.isEmpty) -1 else epochs.head.startOffset
  }

  private def latestOffset(): Long = {
    if (epochs.isEmpty) -1 else epochs.last.startOffset
  }

  private def flush(): Unit = {
    checkpoint.write(epochs)
  }

  def epochChangeMsg(epoch: Int, offset: Long) = s"New: {epoch:$epoch, offset:$offset}, Current: {epoch:${latestEpoch.getOrElse(UNDEFINED_EPOCH)}, offset:$latestOffset} for Partition: $topicPartition"

  def validateAndMaybeWarn(epoch: Int, offset: Long) = {
    assert(epoch >= 0, s"Received a PartitionLeaderEpoch assignment for an epoch < 0. This should not happen. ${epochChangeMsg(epoch, offset)}")
    if (epoch < latestEpoch.getOrElse(UNDEFINED_EPOCH))
      warn(s"Received a PartitionLeaderEpoch assignment for an epoch < latestEpoch. " +
        s"This implies messages have arrived out of order. ${epochChangeMsg(epoch, offset)}")
    else if (offset < latestOffset())
      warn(s"Received a PartitionLeaderEpoch assignment for an offset < latest offset for the most recent, stored PartitionLeaderEpoch. " +
        s"This implies messages have arrived out of order. ${epochChangeMsg(epoch, offset)}")
  }
}

// Mapping of epoch to the first offset of the subsequent epoch
case class EpochEntry(epoch: Int, startOffset: Long)
