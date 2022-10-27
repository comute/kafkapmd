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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.TestSinkConnector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DelegatingClassLoaderTest {

    @Rule
    public TemporaryFolder pluginDir = new TemporaryFolder();

    @Test
    public void testPermittedManifestResources() {
        assertTrue(
            DelegatingClassLoader.serviceLoaderManifestForPlugin("META-INF/services/org.apache.kafka.connect.rest.ConnectRestExtension"));
        assertTrue(
            DelegatingClassLoader.serviceLoaderManifestForPlugin("META-INF/services/org.apache.kafka.common.config.provider.ConfigProvider"));
    }

    @Test
    public void testOtherResources() {
        assertFalse(
            DelegatingClassLoader.serviceLoaderManifestForPlugin("META-INF/services/org.apache.kafka.connect.transforms.Transformation"));
        assertFalse(DelegatingClassLoader.serviceLoaderManifestForPlugin("resource/version.properties"));
    }

    @Test
    public void testLoadingUnloadedPluginClass() {
        TestPlugins.assertAvailable();
        DelegatingClassLoader classLoader = new DelegatingClassLoader(Collections.emptyList());
        classLoader.initLoaders();
        for (String pluginClassName : TestPlugins.pluginClasses()) {
            assertThrows(ClassNotFoundException.class, () -> classLoader.loadClass(pluginClassName));
        }
    }

    @Test
    public void testLoadingPluginClass() throws ClassNotFoundException {
        TestPlugins.assertAvailable();
        DelegatingClassLoader classLoader = new DelegatingClassLoader(TestPlugins.pluginPath());
        classLoader.initLoaders();
        for (String pluginClassName : TestPlugins.pluginClasses()) {
            assertNotNull(classLoader.loadClass(pluginClassName));
            assertNotNull(classLoader.pluginClassLoader(pluginClassName));
        }
    }

    @Test
    public void testLoadingInvalidUberJar() throws Exception {
        pluginDir.newFile("invalid.jar");

        DelegatingClassLoader classLoader = new DelegatingClassLoader(
            Collections.singletonList(pluginDir.getRoot().getAbsolutePath()));
        classLoader.initLoaders();
    }

    @Test
    public void testLoadingPluginDirContainsInvalidJarsOnly() throws Exception {
        pluginDir.newFolder("my-plugin");
        pluginDir.newFile("my-plugin/invalid.jar");

        DelegatingClassLoader classLoader = new DelegatingClassLoader(
            Collections.singletonList(pluginDir.getRoot().getAbsolutePath()));
        classLoader.initLoaders();
    }

    @Test
    public void testLoadingNoPlugins() {
        DelegatingClassLoader classLoader = new DelegatingClassLoader(
            Collections.singletonList(pluginDir.getRoot().getAbsolutePath()));
        classLoader.initLoaders();
    }

    @Test
    public void testLoadingPluginDirEmpty() throws Exception {
        pluginDir.newFolder("my-plugin");

        DelegatingClassLoader classLoader = new DelegatingClassLoader(
            Collections.singletonList(pluginDir.getRoot().getAbsolutePath()));
        classLoader.initLoaders();
    }

    @Test
    public void testLoadingMixOfValidAndInvalidPlugins() throws Exception {
        TestPlugins.assertAvailable();

        pluginDir.newFile("invalid.jar");
        pluginDir.newFolder("my-plugin");
        pluginDir.newFile("my-plugin/invalid.jar");
        Path pluginPath = this.pluginDir.getRoot().toPath();

        for (String sourceJar : TestPlugins.pluginPath()) {
            Path source = new File(sourceJar).toPath();
            Files.copy(source, pluginPath.resolve(source.getFileName()));
        }

        DelegatingClassLoader classLoader = new DelegatingClassLoader(
            Collections.singletonList(pluginDir.getRoot().getAbsolutePath()));
        classLoader.initLoaders();
        for (String pluginClassName : TestPlugins.pluginClasses()) {
            assertNotNull(classLoader.loadClass(pluginClassName));
            assertNotNull(classLoader.pluginClassLoader(pluginClassName));
        }
    }

    @Test
    public void testPluginConflictSameClassSameVersion() {
        DelegatingClassLoader delegatingClassLoader = new DelegatingClassLoader(Collections.emptyList());

        Class<Connector> klass = Connector.class;
        PluginDesc<Connector> connectorDesc1 = new PluginDesc<>(
                klass,
                "1.1.0",
                ClassLoader.getSystemClassLoader()
        );
        delegatingClassLoader.addPlugins(Arrays.asList(connectorDesc1, connectorDesc1), ClassLoader.getSystemClassLoader());
        assertTrue(delegatingClassLoader.reportPluginConflicts().contains(klass.getName()));
    }

    @Test
    public void testPluginConflictSameClassDiffVersions() {
        DelegatingClassLoader delegatingClassLoader = new DelegatingClassLoader(Collections.emptyList());
        PluginDesc<TestSinkConnector> notConflictingPlugin = new PluginDesc<>(
                TestSinkConnector.class,
                "0.0",
                ClassLoader.getSystemClassLoader()
        );
        delegatingClassLoader.addPlugins(Collections.singletonList(notConflictingPlugin), ClassLoader.getSystemClassLoader());

        Class<Connector> conflictingPluginClass = Connector.class;
        PluginDesc<Connector> connectorDesc1 = new PluginDesc<>(
                conflictingPluginClass,
                "1.1.0",
                ClassLoader.getSystemClassLoader()
        );
        delegatingClassLoader.addPlugins(Collections.singletonList(connectorDesc1), ClassLoader.getSystemClassLoader());
        assertTrue(delegatingClassLoader.reportPluginConflicts().isEmpty());

        PluginDesc<Connector> connectorDesc2 = new PluginDesc<>(
                conflictingPluginClass,
                "1.0.0",
                ClassLoader.getSystemClassLoader()
        );
        delegatingClassLoader.addPlugins(Collections.singletonList(connectorDesc2), ClassLoader.getSystemClassLoader());
        String conflictingPluginClassName = conflictingPluginClass.getName();
        assertTrue(delegatingClassLoader.reportPluginConflicts().contains(conflictingPluginClassName));
        assertFalse(delegatingClassLoader.reportPluginConflicts().contains(notConflictingPlugin.className()));
        assertEquals(delegatingClassLoader.usedPluginDesc(conflictingPluginClassName), connectorDesc1);

        PluginDesc<Connector> connectorDesc3 = new PluginDesc<>(
                conflictingPluginClass,
                "2.0.1",
                ClassLoader.getSystemClassLoader()
        );
        delegatingClassLoader.addPlugins(Collections.singletonList(connectorDesc3), ClassLoader.getSystemClassLoader());
        assertTrue(delegatingClassLoader.reportPluginConflicts().contains(conflictingPluginClassName));
        assertFalse(delegatingClassLoader.reportPluginConflicts().contains(notConflictingPlugin.className()));
        assertEquals(delegatingClassLoader.usedPluginDesc(conflictingPluginClassName), connectorDesc3);

        assertEquals(delegatingClassLoader.usedPluginDesc(notConflictingPlugin.className()), notConflictingPlugin);

    }
}
