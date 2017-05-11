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
package org.apache.kafka.connect.runtime.isolation;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

import java.util.Objects;

public class ModuleDesc<T> implements Comparable<ModuleDesc<T>> {
    private final Class<? extends T> klass;
    private final String name;
    private final String version;
    private final DefaultArtifactVersion encodedVersion;
    private final ModuleType type;
    private final String typeName;
    private final String path;

    public ModuleDesc(Class<? extends T> klass, String version, ModuleClassLoader loader) {
        this.klass = klass;
        this.name = klass.getCanonicalName();
        this.version = version;
        this.encodedVersion = new DefaultArtifactVersion(version);
        this.type = ModuleType.from(klass);
        this.typeName = type.toString();
        this.path = loader.path();
    }

    public Class<? extends T> moduleClass() {
        return klass;
    }

    @Override
    public String toString() {
        return "ModuleDesc{" +
                "klass=" + klass +
                ", name='" + name + '\'' +
                ", version='" + version + '\'' +
                ", encodedVersion=" + encodedVersion +
                ", type=" + type +
                ", typeName='" + typeName + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    @JsonProperty("class")
    public String className() {
        return name;
    }

    @JsonProperty("version")
    public String version() {
        return version;
    }

    public ModuleType type() {
        return type;
    }

    @JsonProperty("type")
    public String typeName() {
        return typeName;
    }

    @JsonProperty("path")
    public String path() {
        return typeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ModuleDesc)) {
            return false;
        }

        ModuleDesc<?> that = (ModuleDesc<?>) o;

        if (klass != null ? !klass.equals(that.klass) : that.klass != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return version != null ? version.equals(that.version) : that.version == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(klass, name, version);
    }

    @Override
    public int compareTo(ModuleDesc other) {
        int nameComp = name.compareTo(other.name);
        return nameComp != 0 ? nameComp : encodedVersion.compareTo(other.encodedVersion);
    }
}
