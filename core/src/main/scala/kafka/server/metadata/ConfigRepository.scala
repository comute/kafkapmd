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

package kafka.server.metadata

import java.util.Properties

trait ConfigRepository {
  /**
   * Return a copy of the topic configs for the given topic.  Future changes will not be reflected.
   *
   * @param topicName the name of the topic for which topic configs will be returned
   * @return a copy of the topic configs for the given topic
   */
  def topicConfigs(topicName: String): Properties

  /**
   * Return a copy of the broker configs for the given topic.  Future changes will not be reflected.
   *
   * @param brokerId the id of the broker for which broker configs will be returned
   * @return a copy of the broker configs for the given broker
   */
  def brokerConfigs(brokerId: Int): Properties
}
