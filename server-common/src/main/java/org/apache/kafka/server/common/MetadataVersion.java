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
package org.apache.kafka.server.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.record.RecordVersion;

/**
 * This class contains the different Kafka versions.
 * Right now, we use them for upgrades - users can configure the version of the API brokers will use to communicate between themselves.
 * This is only for inter-broker communications - when communicating with clients, the client decides on the API version.
 *
 * Note that the ID we initialize for each version is important.
 * We consider a version newer than another if it is lower in the enum list (to avoid depending on lexicographic order)
 *
 * Since the api protocol may change more than once within the same release and to facilitate people deploying code from
 * trunk, we have the concept of internal versions (first introduced during the 0.10.0 development cycle). For example,
 * the first time we introduce a version change in a release, say 0.10.0, we will add a config value "0.10.0-IV0" and a
 * corresponding enum constant IBP_0_10_0-IV0. We will also add a config value "0.10.0" that will be mapped to the
 * latest internal version object, which is IBP_0_10_0-IV0. When we change the protocol a second time while developing
 * 0.10.0, we will add a new config value "0.10.0-IV1" and a corresponding enum constant IBP_0_10_0-IV1. We will change
 * the config value "0.10.0" to map to the latest internal version IBP_0_10_0-IV1. The config value of
 * "0.10.0-IV0" is still mapped to IBP_0_10_0-IV0. This way, if people are deploying from trunk, they can use
 * "0.10.0-IV0" and "0.10.0-IV1" to upgrade one internal version at a time. For most people who just want to use
 * released version, they can use "0.10.0" when upgrading to the 0.10.0 release.
 */
public enum MetadataVersion {
    IBP_0_8_0(-1),
    IBP_0_8_1(-1),
    IBP_0_8_2(-1),
    IBP_0_9_0(-1),

    // 0.10.0-IV0 is introduced for KIP-31/32 which changes the message format.
    IBP_0_10_0_IV0(-1),

    // 0.10.0-IV1 is introduced for KIP-36(rack awareness) and KIP-43(SASL handshake).
    IBP_0_10_0_IV1(-1),

    // introduced for JoinGroup protocol change in KIP-62
    IBP_0_10_1_IV0(-1),

    // 0.10.1-IV1 is introduced for KIP-74(fetch response size limit).
    IBP_0_10_1_IV1(-1),

    // introduced ListOffsetRequest v1 in KIP-79
    IBP_0_10_1_IV2(-1),

    // introduced UpdateMetadataRequest v3 in KIP-103
    IBP_0_10_2_IV0(-1),

    // KIP-98 (idempotent and transactional producer support)
    IBP_0_11_0_IV0(-1),

    // introduced DeleteRecordsRequest v0 and FetchRequest v4 in KIP-107
    IBP_0_11_0_IV1(-1),

    // Introduced leader epoch fetches to the replica fetcher via KIP-101
    IBP_0_11_0_IV2(-1),

    // Introduced LeaderAndIsrRequest V1, UpdateMetadataRequest V4 and FetchRequest V6 via KIP-112
    IBP_1_0_IV0(-1),

    // Introduced DeleteGroupsRequest V0 via KIP-229, plus KIP-227 incremental fetch requests,
    // and KafkaStorageException for fetch requests.
    IBP_1_1_IV0(-1),

    // Introduced OffsetsForLeaderEpochRequest V1 via KIP-279 (Fix log divergence between leader and follower after fast leader fail over)
    IBP_2_0_IV0(-1),

    // Several request versions were bumped due to KIP-219 (Improve quota communication)
    IBP_2_0_IV1(-1),

    // Introduced new schemas for group offset (v2) and group metadata (v2) (KIP-211)
    IBP_2_1_IV0(-1),

    // New Fetch, OffsetsForLeaderEpoch, and ListOffsets schemas (KIP-320)
    IBP_2_1_IV1(-1),

    // Support ZStandard Compression Codec (KIP-110)
    IBP_2_1_IV2(-1),

    // Introduced broker generation (KIP-380), and
    // LeaderAdnIsrRequest V2, UpdateMetadataRequest V5, StopReplicaRequest V1
    IBP_2_2_IV0(-1),

    // New error code for ListOffsets when a new leader is lagging behind former HW (KIP-207)
    IBP_2_2_IV1(-1),

    // Introduced static membership.
    IBP_2_3_IV0(-1),

    // Add rack_id to FetchRequest, preferred_read_replica to FetchResponse, and replica_id to OffsetsForLeaderRequest
    IBP_2_3_IV1(-1),

    // Add adding_replicas and removing_replicas fields to LeaderAndIsrRequest
    IBP_2_4_IV0(-1),

    // Flexible version support in inter-broker APIs
    IBP_2_4_IV1(-1),

    // No new APIs, equivalent to 2.4-IV1
    IBP_2_5_IV0(-1),

    // Introduced StopReplicaRequest V3 containing the leader epoch for each partition (KIP-570)
    IBP_2_6_IV0(-1),

    // Introduced feature versioning support (KIP-584)
    IBP_2_7_IV0(-1),

    // Bup Fetch protocol for Raft protocol (KIP-595)
    IBP_2_7_IV1(-1),

    // Introduced AlterPartition (KIP-497)
    IBP_2_7_IV2(-1),

    // Flexible versioning on ListOffsets, WriteTxnMarkers and OffsetsForLeaderEpoch. Also adds topic IDs (KIP-516)
    IBP_2_8_IV0(-1),

    // Introduced topic IDs to LeaderAndIsr and UpdateMetadata requests/responses (KIP-516)
    IBP_2_8_IV1(-1),

    // Introduce AllocateProducerIds (KIP-730)
    IBP_3_0_IV0(1),

    // Introduce ListOffsets V7 which supports listing offsets by max timestamp (KIP-734)
    // Assume message format version is 3.0 (KIP-724)
    IBP_3_0_IV1(2),

    // Adds topic IDs to Fetch requests/responses (KIP-516)
    IBP_3_1_IV0(3),

    // Support for leader recovery for unclean leader election (KIP-704)
    IBP_3_2_IV0(4),

    // KRaft GA
    IBP_3_3_IV0(5);

    private final Optional<Short> metadataVersion;
    private final String shortVersion;
    private final String version;

    MetadataVersion(int metadataVersion) {
        if (metadataVersion > 0) {
            this.metadataVersion = Optional.of((short) metadataVersion);
        } else {
            this.metadataVersion = Optional.empty();
        }

        Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)(?:IV(\\d))?");
        Matcher matcher = versionPattern.matcher(this.name());
        if (matcher.find()) {
            String withoutIV = matcher.group(1);
            // remove any trailing underscores
            if (withoutIV.endsWith("_")) {
                withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
            }
            shortVersion = withoutIV.replace("_", ".");

            if (matcher.group(2) != null) { // versions less than IBP_0_10_0_IV0 do not have IVs
                version = String.format("%s-IV%s", shortVersion, matcher.group(2));
            } else {
                version = shortVersion;
            }
        } else {
            throw new IllegalArgumentException("Metadata version: " + this.name() + " does not fit "
                + "the accepted pattern.");
        }
    }

    public Optional<Short> metadataVersion() {
        return metadataVersion;
    }

    public boolean isSaslInterBrokerHandshakeRequestEnabled() {
        return this.isAtLeast(IBP_0_10_0_IV1);
    }

    public boolean isOffsetForLeaderEpochSupported() {
        return this.isAtLeast(IBP_0_11_0_IV2);
    }

    public boolean isFeatureVersioningSupported() {
        return this.isAtLeast(IBP_2_7_IV0);
    }

    public boolean isTruncationOnFetchSupported() {
        return this.isAtLeast(IBP_2_7_IV1);
    }

    public boolean isAlterIsrSupported() {
        return this.isAtLeast(IBP_2_7_IV2);
    }

    public boolean isTopicIdsSupported() {
        return this.isAtLeast(IBP_2_8_IV0);
    }

    public boolean isAllocateProducerIdsSupported() {
        return this.isAtLeast(IBP_3_0_IV0);
    }


    public RecordVersion recordVersion() {
        if (this.isLessThan(IBP_0_10_0_IV0)) {
            // IBPs less than IBP_0_10_0_IV0 use Record Version V0
            return RecordVersion.V0;
        } else if (this.isLessThan(IBP_0_11_0_IV0)) {
            // IBPs >= IBP_0_10_0_IV0 and less than IBP_0_11_0_IV0 use V1
            return RecordVersion.V1;
        } else {
            // all greater IBPs use V2
            return RecordVersion.V2;
        }
    }

    private static final Map<String, MetadataVersion> IBP_VERSIONS;
    static {
        {
            IBP_VERSIONS = new HashMap<>();
            Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)(?:IV(\\d))?");
            Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
            for (MetadataVersion version : MetadataVersion.values()) {
                Matcher matcher = versionPattern.matcher(version.name());
                if (matcher.find()) {
                    String withoutIV = matcher.group(1);
                    // remove any trailing underscores
                    if (withoutIV.endsWith("_")) {
                        withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
                    }
                    String shortVersion = withoutIV.replace("_", ".");

                    String normalizedVersion;
                    if (matcher.group(2) != null) {
                        normalizedVersion = String.format("%s-IV%s", shortVersion, matcher.group(2));
                    } else {
                        normalizedVersion = shortVersion;
                    }
                    maxInterVersion.compute(shortVersion, (__, currentVersion) -> {
                        if (currentVersion == null) {
                            return version;
                        } else if (version.compareTo(currentVersion) > 0) {
                            return version;
                        } else {
                            return currentVersion;
                        }
                    });
                    IBP_VERSIONS.put(normalizedVersion, version);
                } else {
                    throw new IllegalArgumentException("Metadata version: " + version.name() + " does not fit "
                            + "any of the accepted patterns.");
                }
            }
            IBP_VERSIONS.putAll(maxInterVersion);
        }
    }

    public String shortVersion() {
        return shortVersion;
    }

    public String version() {
        return version;
    }

    /**
     * Return an `MetadataVersion` instance for `versionString`, which can be in a variety of formats (e.g. "0.8.0", "0.8.0.x",
     * "0.10.0", "0.10.0-IV1"). `IllegalArgumentException` is thrown if `versionString` cannot be mapped to an `MetadataVersion`.
     */
    public static MetadataVersion fromVersionString(String versionString) {
        String[] versionSegments = versionString.split(Pattern.quote("."));
        int numSegments = (versionString.startsWith("0.")) ? 3 : 2;
        String key;
        if (numSegments >= versionSegments.length) {
            key = versionString;
        } else {
            key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
        }
        return Optional.ofNullable(IBP_VERSIONS.get(key)).orElseThrow(() ->
            new IllegalArgumentException("Version " + versionString + " is not a valid version")
        );
    }

    /**
     * Return the minimum `MetadataVersion` that supports `RecordVersion`.
     */
    public static MetadataVersion minSupportedFor(RecordVersion recordVersion) {
        switch (recordVersion) {
            case V0:
                return IBP_0_8_0;
            case V1:
                return IBP_0_10_0_IV0;
            case V2:
                return IBP_0_11_0_IV0;
            default:
                throw new IllegalArgumentException("Invalid message format version " + recordVersion);
        }
    }

    public static MetadataVersion latest() {
        MetadataVersion[] values = MetadataVersion.values();
        return values[values.length - 1];
    }

    public boolean isAtLeast(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }

    @Override
    public String toString() {
        return version;
    }
}
