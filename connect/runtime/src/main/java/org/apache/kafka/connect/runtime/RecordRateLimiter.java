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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.util.Collection;
import java.util.Map;

/**
 * Simple RateLimiter in terms of records-per-second.
 *
 */
public class RecordRateLimiter<R extends ConnectRecord> extends CountingRateLimiter<R> {

    public final static String RECORD_RATE_LIMIT_CONFIG = "record.rate.limit";
    public final static String RECORD_RATE_LIMIT_DOC = "Max records per second allowed through each Task.";
    public final static String RECORD_RATE_LIMIT_DISPLAY = "Records per second";

    public final static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            RECORD_RATE_LIMIT_CONFIG,
            ConfigDef.Type.DOUBLE,
            -1,
            ConfigDef.Importance.LOW,
            RECORD_RATE_LIMIT_DOC,
            ConnectorConfig.RATE_LIMITS_GROUP,
            1,
            ConfigDef.Width.LONG,
            RECORD_RATE_LIMIT_DISPLAY);

    @Override
    public void accumulate(Collection<R> records) {
        count(records.size());
    }

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(config(), props, true);
        setTargetRate(config.getDouble(RECORD_RATE_LIMIT_CONFIG));
    }
 
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
