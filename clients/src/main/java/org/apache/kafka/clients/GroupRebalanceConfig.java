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
package org.apache.kafka.clients;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.requests.JoinGroupRequest;

/**
 * Class to extract group rebalance related configs.
 */
public class GroupRebalanceConfig {

    public enum ProtocolType {
        CONSUMER,
        CONNECT;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    private int sessionTimeoutMs;
    public final int rebalanceTimeoutMs;
    private int heartbeatIntervalMs;
    public final String groupId;
    public final Optional<String> groupInstanceId;
    public final long retryBackoffMs;
    public final boolean leaveGroupOnClose;

    private Map<String, ?> originals;
    public final String clientId;
    private final boolean enableDynamicConfig;
    private boolean updateCoordinatorSessionTimeout;

    public GroupRebalanceConfig(AbstractConfig config, ProtocolType protocolType) {
        this.sessionTimeoutMs = config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG);

        // Consumer and Connect use different config names for defining rebalance timeout
        if (protocolType == ProtocolType.CONSUMER) {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        } else {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG);
        }

        this.heartbeatIntervalMs = config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG);
        this.groupId = config.getString(CommonClientConfigs.GROUP_ID_CONFIG);
        validateSessionAndHeartbeat(this.sessionTimeoutMs, this.heartbeatIntervalMs);
        this.updateCoordinatorSessionTimeout = false;

        this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        this.enableDynamicConfig = config.getBoolean(CommonClientConfigs.ENABLE_DYNAMIC_CONFIG_CONFIG);
        this.originals = config.values();

        // Static membership is only introduced in consumer API.
        if (protocolType == ProtocolType.CONSUMER) {
            String groupInstanceId = config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG);
            if (groupInstanceId != null) {
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
                this.groupInstanceId = Optional.of(groupInstanceId);
            } else {
                this.groupInstanceId = Optional.empty();
            }
        } else {
            this.groupInstanceId = Optional.empty();
        }

        this.retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);

        // Internal leave group config is only defined in Consumer.
        if (protocolType == ProtocolType.CONSUMER) {
            this.leaveGroupOnClose = config.getBoolean("internal.leave.group.on.close");
        } else {
            this.leaveGroupOnClose = true;
        }
    }

    // For testing purpose.
    public GroupRebalanceConfig(final int sessionTimeoutMs,
                                final int rebalanceTimeoutMs,
                                final int heartbeatIntervalMs,
                                String groupId,
                                Optional<String> groupInstanceId,
                                long retryBackoffMs,
                                boolean leaveGroupOnClose) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.retryBackoffMs = retryBackoffMs;
        this.leaveGroupOnClose = leaveGroupOnClose;
        validateSessionAndHeartbeat(sessionTimeoutMs, heartbeatIntervalMs);
        this.clientId = "";
        this.enableDynamicConfig = false;
    }

    /**
     * Validate the dynamic configs and then reconfigure
     * @param configs dynamic configs
     * @return true if the session timeout has been dynamically updated and a new join group request needs to be sent to broker
     */
    public void setDynamicConfigs(Map<String, String> configs) {
        int newSessionTimeoutMs = Integer.parseInt(
                configs.getOrDefault(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, 
                originals.get(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG).toString()));
        int newHeartbeatIntervalMs = Integer.parseInt(
                configs.getOrDefault(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG,
                originals.get(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG).toString()));
        validateSessionAndHeartbeat(newSessionTimeoutMs, newHeartbeatIntervalMs);
        this.heartbeatIntervalMs = newHeartbeatIntervalMs;
        if (newSessionTimeoutMs != sessionTimeoutMs) {
            this.updateCoordinatorSessionTimeout = true;
        }
        this.sessionTimeoutMs = newSessionTimeoutMs;
    }

    public boolean coordinatorNeedsSessionTimeoutUpdate() {
        return this.updateCoordinatorSessionTimeout;
    }

    public void coordinatorUpdated() {
        this.updateCoordinatorSessionTimeout = false;
    }

    public int getSessionTimout() {
        return this.sessionTimeoutMs;
    }

    public int getHeartbeatInterval() {
        return this.heartbeatIntervalMs;
    }

    public boolean enableDynamicConfig() {
        return this.enableDynamicConfig;
    }

    private void validateSessionAndHeartbeat(int sessionTimeoutMs, int heartbeatIntervalMs) {
        if (heartbeatIntervalMs >= sessionTimeoutMs)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");
    }
}
