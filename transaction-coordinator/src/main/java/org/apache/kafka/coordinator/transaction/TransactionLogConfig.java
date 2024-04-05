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
package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.server.config.ServerTopicConfigSynonyms;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

public class TransactionLogConfig {
    // Log-level config default values
    public static final String TRANSACTIONS_TOPIC_PARTITIONS_CONFIG = "transaction.state.log.num.partitions";
    public static final int TRANSACTIONS_TOPIC_PARTITIONS_DEFAULT = 50;
    public static final String TRANSACTIONS_TOPIC_PARTITIONS_DOC = "The number of partitions for the transaction topic (should not change after deployment).";

    public static final String TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG = "transaction.state.log.segment.bytes";
    public static final int TRANSACTIONS_TOPIC_SEGMENT_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final String TRANSACTIONS_TOPIC_SEGMENT_BYTES_DOC = "The transaction topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads";

    public static final String TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG = "transaction.state.log.replication.factor";
    public static final short TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the transaction topic (set higher to ensure availability). " +
            "Internal topic creation will fail until the cluster size meets this replication factor requirement.";

    public static final String TRANSACTIONS_TOPIC_MIN_ISR_CONFIG = "transaction.state.log.min.isr";
    public static final int TRANSACTIONS_TOPIC_MIN_ISR_DEFAULT = 2;
    public static final String TRANSACTIONS_TOPIC_MIN_ISR_DOC = "Overridden " + ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG) + " config for the transaction topic.";

    public static final String TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG = "transaction.state.log.load.buffer.size";
    public static final int TRANSACTIONS_LOAD_BUFFER_SIZE_DEFAULT = 5 * 1024 * 1024;
    public static final String TRANSACTIONS_LOAD_BUFFER_SIZE_DOC = "Batch size for reading from the transaction log segments when loading producer ids and transactions into the cache (soft-limit, overridden if records are too large).";

    public static final String TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG = ProducerStateManagerConfig.TRANSACTION_VERIFICATION_ENABLED;
    public static final boolean TRANSACTION_PARTITION_VERIFICATION_ENABLE_DEFAULT = true;
    public static final String TRANSACTION_PARTITION_VERIFICATION_ENABLE_DOC = "Enable verification that checks that the partition has been added to the transaction before writing transactional records to the partition";

    public static final String PRODUCER_ID_EXPIRATION_MS_CONFIG = ProducerStateManagerConfig.PRODUCER_ID_EXPIRATION_MS;
    public static final int PRODUCER_ID_EXPIRATION_MS_DEFAULT = 86400000;
    public static final String PRODUCER_ID_EXPIRATION_MS_DOC = "The time in ms that a topic partition leader will wait before expiring producer IDs. Producer IDs will not expire while a transaction associated to them is still ongoing. " +
            "Note that producer IDs may expire sooner if the last write from the producer ID is deleted due to the topic's retention settings. Setting this value the same or higher than " +
            "<code>delivery.timeout.ms</code> can help prevent expiration during retries and protect against message duplication, but the default should be reasonable for most use cases.";
    public static final String PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG = "producer.id.expiration.check.interval.ms";
    public static final int PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT = 600000;
    public static final String PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DOC = "The interval at which to remove producer IDs that have expired due to <code>producer.id.expiration.ms</code> passing.";
}
