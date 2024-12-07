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

package org.apache.kafka.connect.util;

import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;

public class PluginVersionUtils {

    public static VersionRange connectorVersionRequirement(String version) throws InvalidVersionSpecificationException {
        if (version == null || version.equals("latest")) {
            return null;
        }
        version = version.trim();

        // check first if the given version is valid
        VersionRange.createFromVersionSpec(version);

        // now if the version is not enclosed we consider it as a hard requirement and enclose it in []
        if (!version.startsWith("[") && !version.startsWith("(")) {
            version = "[" + version + "]";
        }
        return VersionRange.createFromVersionSpec(version);
    }
}