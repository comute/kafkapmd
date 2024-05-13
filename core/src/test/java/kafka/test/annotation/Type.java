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

package kafka.test.annotation;

import kafka.test.ClusterConfig;
import kafka.test.junit.RaftClusterInvocationContext;
import kafka.test.junit.ZkClusterInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;

import java.util.List;
import java.util.ArrayList;

/**
 * The type of cluster config being requested. Used by {@link kafka.test.ClusterConfig} and the test annotations.
 */
public enum Type {
    KRAFT {
        @Override
        public List<TestTemplateInvocationContext> invocationContexts(String baseDisplayName, ClusterConfig config) {
            List<TestTemplateInvocationContext> ret = new ArrayList<>();
            ret.add(new RaftClusterInvocationContext(baseDisplayName, config, false));
            return ret;
        }
    },
    CO_KRAFT {
        @Override
        public List<TestTemplateInvocationContext> invocationContexts(String baseDisplayName, ClusterConfig config) {
            List<TestTemplateInvocationContext> ret = new ArrayList<>();
            ret.add(new RaftClusterInvocationContext(baseDisplayName, config, true));
            return ret;
        }
    },
    ZK {
        @Override
        public List<TestTemplateInvocationContext> invocationContexts(String baseDisplayName, ClusterConfig config) {
            List<TestTemplateInvocationContext> ret = new ArrayList<>();
            ret.add(new ZkClusterInvocationContext(baseDisplayName, config));
            return ret;
        }
    };

    public abstract List<TestTemplateInvocationContext> invocationContexts(String baseDisplayName, ClusterConfig config);
}
