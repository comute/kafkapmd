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

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Modules {
    private static final Logger log = LoggerFactory.getLogger(Modules.class);

    private final List<String> moduleTopPaths;
    private final Map<ModuleClassLoader, URL[]> loaders;
    private final Map<String, ModuleClassLoader> modulesToLoaders;
    private final DelegatingClassLoader delegatingLoader;
    private final Connectors connectors;
    private final Converters converters;
    private final Transformations transformations;

    public Modules(WorkerConfig workerConfig) {
        loaders = new HashMap<>();
        modulesToLoaders = new HashMap<>();
        moduleTopPaths = workerConfig.getList(WorkerConfig.MODULE_PATH_CONFIG);
        delegatingLoader = new DelegatingClassLoader(moduleTopPaths);
        connectors = new Connectors(delegatingLoader);
        converters = new Converters(delegatingLoader);
        transformations = new Transformations(delegatingLoader);
    }

    public DelegatingClassLoader getDelegatingLoader() {
        return delegatingLoader;
    }

    public Map<ModuleClassLoader, URL[]> getLoaders() {
        return loaders;
    }

    public Connector newConnector(String connectorClassOrAlias) {
        return connectors.newConnector(connectorClassOrAlias);
    }

    public Task newTask(Class<? extends Task> taskClass) {
        return connectors.newTask(taskClass);
    }

    public Converter newConverter(String converterClassOrAlias) {
        return converters.newConverter(converterClassOrAlias);
    }

    public <R extends ConnectRecord<R>> Transformation<R> newTranformations(
            String converterClassOrAlias
    ) {
        return transformations.newTransformation(converterClassOrAlias);
    }

    private static String connectorNames(Collection<Class<? extends Connector>> connectors) {
        StringBuilder names = new StringBuilder();
        for (Class<?> c : connectors)
            names.append(c.getName()).append(", ");
        return names.substring(0, names.toString().length() - 2);
    }
}
