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

package kafka.log

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util

import kafka.utils.{CoreUtils, nonthreadsafe}
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.{ByteUtils, Utils}

trait CleanerCache {
  /** The maximum number of entries this map can contain */
  def slots: Int

  /**
   * Put this record in the cache, but only if it is greater than
   * the currently associated record (if any).
   * A record is considered to be greater than another if it has a larger version
   * than the currently cached record, or if no cached record exists at all.
   * The version is determined based on the strategy used when creating this cache.
   *
   * @param record  The record
   * @return True if the record was inserted, false otherwise
   */
  def put(record: Record): Boolean

  /**
   * Get the cached value associated with this key.
   *
   * @param key The key
   * @return The cached value associated with this key or -1 if the key is not found
   */
  def get(key: ByteBuffer): Long

  /**
   * Sets the passed value as the latest offset.
   * This value is not set if it is lesser than the currently kept value.
   */
  def updateLatestOffset(offset: Long)

  /** Change the salt used for key hashing making all existing keys unfindable. */
  def clear()

  /** The number of entries put into the map (note that not all may remain). */
  def size: Int

  def utilization: Double = size.toDouble / slots

  /** The latest offset put into the map. */
  def latestOffset: Long

  /**
   * @return True if the passed record has a larger version than the currently
   *         cached record, or if no cached record exists at all.
   */
  def greater(record: Record): Boolean
}

object Constants {
  val OffsetStrategy: String = Defaults.CompactionStrategy
  val TimestampStrategy: String = "timestamp"
}

/**
 * A hash table used for deduplicating the log.
 * This hash table uses a cryptographically secure hash of the key as a proxy
 * for the key for comparisons and to save space on object overhead.
 * Collisions are resolved by probing.
 * This hash table does not support deletes.
 *
 * @param memory        The amount of memory this map can use
 * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512
 * @param strategy      The compaction strategy for this cleaner to adopt
 */
@nonthreadsafe
class SkimpyCleanerCache(val memory: Int, val hashAlgorithm: String = "MD5", val strategy: String = Constants.OffsetStrategy) extends CleanerCache {
  private val bytes = ByteBuffer.allocate(memory)

  /* the hash algorithm instance to use, default is MD5 */
  private val digest = MessageDigest.getInstance(hashAlgorithm)

  /* the number of bytes for this hash algorithm */
  private val hashSize = digest.getDigestLength

  /* create some hash buffers to avoid reallocating each time */
  private val hash1 = new Array[Byte](hashSize)
  private val hash2 = new Array[Byte](hashSize)

  /* number of entries put into the map */
  private var entries = 0

  /* number of lookups on the map */
  private var lookups = 0L

  /* the number of probes for all lookups */
  private var probes = 0L

  /* the latest offset written into the map */
  private var lastOffset = -1L

  /**
   * The number of bytes of space each entry uses.
   * This evaluates to the number of bytes in the hash plus 8 bytes for the value.
   */
  val bytesPerEntry: Int = hashSize + java.lang.Long.BYTES

  val slots: Int = memory / bytesPerEntry

  override def put(record: Record): Boolean = {
    require(entries < slots, "Attempt to add a new entry to a full cache.")
    if (!record.hasKey || !greater(record)) {
      return false
    }
    val key = record.key
    val value = extractValue(record)
    lookups += 1
    hashInto(key, hash1)
    // probe until we find the first empty slot
    var attempt = 0
    var pos = positionOf(hash1, attempt)
    while (!isEmpty(pos)) {
      bytes.position(pos)
      bytes.get(hash2)
      if (util.Arrays.equals(hash1, hash2)) {
        // we found an existing entry, overwrite it and return (size does not change)
        bytes.putLong(value)
        updateLatestOffset(record.offset)
        return true
      }
      attempt += 1
      pos = positionOf(hash1, attempt)
    }
    // found an empty slot, update it--size grows by 1
    bytes.position(pos)
    bytes.put(hash1)
    bytes.putLong(value)
    updateLatestOffset(record.offset)
    entries += 1
    true
  }

  override def get(key: ByteBuffer): Long = {
    lookups += 1
    hashInto(key, hash1)
    // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
    var attempt = 0
    var pos = 0
    // we need to guard against attempt integer overflow if the map is full
    // limit attempt to number of slots once positionOf(..) enters linear search mode
    val maxAttempts = slots + hashSize - 4
    do {
      if (attempt >= maxAttempts)
        return -1L
      pos = positionOf(hash1, attempt)
      bytes.position(pos)
      if (isEmpty(pos))
        return -1L
      bytes.get(hash2)
      attempt += 1
    } while (!util.Arrays.equals(hash1, hash2))
    bytes.getLong()
  }

  override def greater(record: Record): Boolean = {
    val recordValue = extractValue(record)
    val cachedValue = get(record.key)
    cachedValue < 0 || cachedValue < recordValue
  }

  override def clear() {
    this.entries = 0
    this.lookups = 0L
    this.probes = 0L
    this.lastOffset = -1L
    util.Arrays.fill(bytes.array, bytes.arrayOffset, bytes.arrayOffset + bytes.limit(), 0.toByte)
  }

  override def size: Int = entries

  /** The rate of collisions in the lookups. */
  def collisionRate: Double =
    (this.probes - this.lookups) / this.lookups.toDouble

  override def latestOffset: Long = lastOffset

  override def updateLatestOffset(offset: Long): Unit = {
    if (lastOffset < offset) lastOffset = offset
  }

  /** @return The value to cache as it is extracted from the record. */
  private def extractValue(record: Record): Long = {
    if (strategy == null || Constants.OffsetStrategy.equalsIgnoreCase(strategy)) {
      // using value based on record offset
      return record.offset
    }
    if (Constants.TimestampStrategy.equalsIgnoreCase(strategy)) {
      // using value based on record timestamp
      return record.timestamp
    }
    if (record == null || record.headers() == null || record.headers.isEmpty) {
      // not able to determine the version of this header
      return -1
    }
    // using value based on a custom header
    record.headers()
      .filter(it => it.value != null && it.value.nonEmpty)
      .find(it => strategy.trim.equalsIgnoreCase(it.key.trim))
      .map(it => ByteBuffer.wrap(it.value))
      .map(it => ByteUtils.readVarlong(it))
      .getOrElse(-1L)
  }

  /** Check that there is no entry at the given position . */
  private def isEmpty(position: Int): Boolean = bytes.getLong(position) == 0 &&
      bytes.getLong(position + java.lang.Long.BYTES) == 0 &&
      bytes.getLong(position + 16) == 0

  /**
    * Calculate the ith probe position. We first try reading successive integers from the hash itself
    * then if all of those fail we degrade to linear probing.
    *
    * @param hash    The hash of the key to find the position for
    * @param attempt The ith probe
    * @return The byte offset in the buffer at which the ith probing for the given hash would reside
    */
  private def positionOf(hash: Array[Byte], attempt: Int): Int = {
    val probe = CoreUtils.readInt(hash, math.min(attempt, hashSize - 4)) + math.max(0, attempt - hashSize + 4)
    val slot = Utils.abs(probe) % slots
    this.probes += 1
    slot * bytesPerEntry
  }

  /**
    * Evaluates the key into a hash and places it into the given output buffer.
    *
    * @param key    The key to hash
    * @param buffer The buffer to store the hash into
    */
  private def hashInto(key: ByteBuffer, buffer: Array[Byte]) {
    key.mark()
    digest.update(key)
    key.reset()
    digest.digest(buffer, 0, hashSize)
  }

}
