# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.consume_bench_workload import ConsumeBenchWorkloadService, ConsumeBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.kafka import KafkaService
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService

import json
import time


class ReplicaScaleTest(Test):
    def __init__(self, test_context):
        super(ReplicaScaleTest, self).__init__(test_context=test_context)
        self.test_context = test_context
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=8, zk=self.zk)

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def teardown(self):
        # Need to increase the timeout due to partition count
        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=False, timeout_sec=60)
        self.kafka.stop()
        self.zk.stop()

    @cluster(num_nodes=12)
    @parametrize(topic_count=1000, partition_count=34, replication_factor=3)
    def test_100k_bench(self, topic_count, partition_count, replication_factor):
        t0 = time.time()
        for i in range(topic_count):
            topic = "100k_replicas_bench%d" % i
            print("Creating topic %s" % topic)  # Force some stdout for Jenkins
            topic_cfg = {
                "topic": topic,
                "partitions": partition_count,
                "replication-factor": replication_factor,
                "configs": {"min.insync.replicas": 1}
            }
            self.kafka.create_topic(topic_cfg, describe=False)

        t1 = time.time()
        self.logger.info("Time to create topics: %d" % (t1-t0))

        producer_workload_service = ProduceBenchWorkloadService(self.test_context, self.kafka)
        consumer_workload_service = ConsumeBenchWorkloadService(self.test_context, self.kafka)
        trogdor = TrogdorService(context=self.test_context,
                                 client_services=[self.kafka, producer_workload_service, consumer_workload_service])
        trogdor.start()

        produce_spec = ProduceBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                producer_workload_service.producer_node,
                                                producer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=10000,
                                                max_messages=3400000,
                                                producer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                inactive_topics={},
                                                active_topics={"100k_replicas_bench[0-2]": {
                                                    "numPartitions": partition_count, "replicationFactor": replication_factor
                                                }})
        produce_workload = trogdor.create_task("100k-replicas-produce-workload", produce_spec)
        produce_workload.wait_for_done(timeout_sec=600)
        self.logger.info("Completed produce bench")

        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                consumer_workload_service.consumer_node,
                                                consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=10000,
                                                max_messages=3400000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                active_topics=["100k_replicas_bench[0-2]"])
        consume_workload = trogdor.create_task("100k-replicas-consume_workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=600)
        self.logger.info("Completed consume bench")

        trogdor.stop()

    @cluster(num_nodes=12)
    @parametrize(topic_count=1000, partition_count=34, replication_factor=3)
    def test_100k_clean_bounce(self, topic_count, partition_count, replication_factor):
        t0 = time.time()
        for i in range(topic_count):
            topic = "topic-%04d" % i
            print("Creating topic %s" % topic)  # Force some stdout for Jenkins
            topic_cfg = {
                "topic": topic,
                "partitions": partition_count,
                "replication-factor": replication_factor,
                "configs": {"min.insync.replicas": 1}
            }
            self.kafka.create_topic(topic_cfg)
        t1 = time.time()
        self.logger.info("Time to create topics: %d" % (t1-t0))

        restart_times = []
        for node in self.kafka.nodes:
            t2 = time.time()
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=600)
            self.kafka.start_node(node, timeout_sec=600)
            t3 = time.time()
            restart_times.append(t3-t2)
            self.logger.info("Time to restart %s: %d" % (node.name, t3-t2))

        self.logger.info("Restart times: %s" % restart_times)

        t0 = time.time()
        for i in range(topic_count):
            topic = "topic-%04d" % i
            self.logger.info("Deleting topic %s" % topic)
            self.kafka.delete_topic(topic)
        t1 = time.time()
        self.logger.info("Time to delete topics: %d" % (t1-t0))
