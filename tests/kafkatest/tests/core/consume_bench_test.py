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

import json
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.consume_bench_workload import ConsumeBenchWorkloadService, ConsumeBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.zookeeper import ZookeeperService


class ConsumeBenchTest(Test):
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ConsumeBenchTest, self).__init__(test_context)
        self.zk = ZookeeperService(test_context, num_nodes=3)
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk)
        self.producer_workload_service = ProduceBenchWorkloadService(test_context, self.kafka)
        self.consumer_workload_service = ConsumeBenchWorkloadService(test_context, self.kafka)
        self.active_topics = {"consume_bench_topic[0-5]": {"numPartitions": 1, "replicationFactor": 3}}
        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka, self.producer_workload_service,
                                                       self.consumer_workload_service])

    def setUp(self):
        self.trogdor.start()
        self.zk.start()
        self.kafka.start()

    def teardown(self):
        self.trogdor.stop()
        self.kafka.stop()
        self.zk.stop()

    def test_consume_bench(self):
        """
        Serially runs a ProduceBench workload to produce messages to a topic
            and then a ConsumeBench workload to consume said messages
        """
        produce_spec = ProduceBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.producer_workload_service.producer_node,
                                                self.producer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=10000,
                                                producer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                inactive_topics={},
                                                active_topics=self.active_topics)
        produce_workload = self.trogdor.create_task("produce_workload", produce_spec)
        produce_workload.wait_for_done(timeout_sec=180)
        self.logger.debug("Produce workload finished")
        consume_spec = ConsumeBenchWorkloadSpec(0, TaskSpec.MAX_DURATION_MS,
                                                self.consumer_workload_service.consumer_node,
                                                self.consumer_workload_service.bootstrap_servers,
                                                target_messages_per_sec=1000,
                                                max_messages=10000,
                                                consumer_conf={},
                                                admin_client_conf={},
                                                common_client_conf={},
                                                active_topics=self.active_topics)
        consume_workload = self.trogdor.create_task("consume_workload", consume_spec)
        consume_workload.wait_for_done(timeout_sec=180)
        self.logger.debug("Consume workload finished")
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" % json.dumps(tasks, sort_keys=True, indent=2))