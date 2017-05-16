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
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DelegatingClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClassLoader.class);

    private final ConcurrentMap<String, SortedMap<PluginDesc<?>, PluginClassLoader>> pluginLoaders;
    private final SortedSet<PluginDesc<Connector>> connectors;
    private final SortedSet<PluginDesc<Converter>> converters;
    private final SortedSet<PluginDesc<Transformation>> transformations;
    private final List<String> pluginPaths;
    private final Map<Path, PluginClassLoader> activePaths;
    private final Map<Path, Void> inactivePaths;

    public DelegatingClassLoader(List<String> pluginPaths, ClassLoader parent) {
        super(new URL[0], parent);
        this.pluginPaths = pluginPaths;
        this.pluginLoaders = new ConcurrentHashMap<>();
        this.activePaths = new HashMap<>();
        this.inactivePaths = new HashMap<>();
        this.connectors = new TreeSet<>();
        this.converters = new TreeSet<>();
        this.transformations = new TreeSet<>();
    }

    public DelegatingClassLoader(List<String> pluginPaths) {
        this(pluginPaths, ClassLoader.getSystemClassLoader());
    }

    private static boolean isConcrete(Class<?> cls) {
        final int mod = cls.getModifiers();
        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod);
    }

    private static List<Path> pluginDirs(Path topDir) throws IOException {
        DirectoryStream.Filter<Path> dirFilter = new DirectoryStream.Filter<Path>() {
            public boolean accept(Path path) throws IOException {
                return Files.isDirectory(path);
            }
        };

        List<Path> dirs = new ArrayList<>();
        // Non-recursive for now
        try (DirectoryStream<Path> listing = Files.newDirectoryStream(topDir, dirFilter)) {
            for (Path dir : listing) {
                dirs.add(dir);
            }
        }
        return dirs;
    }

    private static List<URL> jarPaths(Path pluginDir) throws IOException {
        DirectoryStream.Filter<Path> jarFilter = new DirectoryStream.Filter<Path>() {
            public boolean accept(Path file) throws IOException {
                return file.toString().toLowerCase(Locale.ROOT).endsWith(".jar");
            }
        };

        List<URL> jars = new ArrayList<>();
        try (DirectoryStream<Path> listing = Files.newDirectoryStream(pluginDir, jarFilter)) {
            for (Path jar : listing) {
                jars.add(jar.toUri().toURL());
            }
        }
        return jars;
    }

    public void addAlias(PluginDesc<?> plugin, String alias) {
        SortedMap<PluginDesc<?>, PluginClassLoader> inner = pluginLoaders.get(plugin.className());
        if (inner != null) {
            pluginLoaders.putIfAbsent(alias, inner);
        }
    }

    private <T> void addPlugins(Collection<PluginDesc<T>> plugins, PluginClassLoader loader) {
        for (PluginDesc<T> plugin : plugins) {
            String pluginClassName = plugin.className();
            SortedMap<PluginDesc<?>, PluginClassLoader> inner = pluginLoaders.get(pluginClassName);
            if (inner == null) {
                inner = new TreeMap<>();
                pluginLoaders.put(pluginClassName, inner);
            }
            inner.put(plugin, loader);
        }
    }

    public void initLoaders() {
        for (String path : pluginPaths) {
            try {
                Path pluginPath = Paths.get(path).toAbsolutePath();
                if (Files.isDirectory(pluginPath)) {
                    for (Path dir : pluginDirs(pluginPath)) {
                        log.info("Loading dir: {}", dir);
                        URL[] jars = jarPaths(dir).toArray(new URL[0]);
                        if (log.isDebugEnabled()) {
                            log.debug("Loading jars: {}", Arrays.toString(jars));
                        }
                        PluginClassLoader loader = newPluginClassLoader(dir.toUri().toURL(), jars);
                        log.info("Using loader: {}", loader);
                        PluginScanResult plugins = scanPluginPath(loader, jars);

                        if (plugins.isEmpty()) {
                            inactivePaths.put(dir, null);
                        } else {
                            activePaths.put(dir, loader);
                        }

                        addPlugins(plugins.connectors(), loader);
                        connectors.addAll(plugins.connectors());
                        addPlugins(plugins.converters(), loader);
                        converters.addAll(plugins.converters());
                        addPlugins(plugins.transformations(), loader);
                        transformations.addAll(plugins.transformations());
                    }
                }
            } catch (InvalidPathException | MalformedURLException e) {
                log.warn("Invalid path in plugin path: {}. Ignoring.", path);
            } catch (IOException e) {
                log.warn("Could not get listing for plugin path: {}. Ignoring.", path);
            } catch (InstantiationException | IllegalAccessException e) {
                log.warn("Could not instantiate plugins in: {}. Ignoring: {}", path, e);
            }
        }
    }

    private static PluginClassLoader newPluginClassLoader(
            final URL pluginLocation,
            final URL[] jars
    ) {
        return (PluginClassLoader) AccessController.doPrivileged(
                new PrivilegedAction() {
                    @Override
                    public Object run() {
                        return new PluginClassLoader(pluginLocation, jars);
                    }
                }
        );
    }

    public Set<PluginDesc<Connector>> connectors() {
        return connectors;
    }

    public Set<PluginDesc<Converter>> converters() {
        return converters;
    }

    public Set<PluginDesc<Transformation>> transformations() {
        return transformations;
    }

    private PluginScanResult scanPluginPath(
            PluginClassLoader loader,
            URL[] urls
    ) throws InstantiationException, IllegalAccessException {
        //log.info("Scanning plugin path urls: " + Arrays.toString(urls));
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setClassLoaders(new PluginClassLoader[]{loader});
        builder.addUrls(urls);
        Reflections reflections = new Reflections(builder);

        return new PluginScanResult(
                getPluginDesc(reflections, Connector.class, loader),
                getPluginDesc(reflections, Converter.class, loader),
                getPluginDesc(reflections, Transformation.class, loader)
        );
    }

    private <T> Collection<PluginDesc<T>> getPluginDesc(
            Reflections reflections,
            Class<T> klass,
            PluginClassLoader loader
    ) throws InstantiationException, IllegalAccessException {
        Set<Class<? extends T>> plugins = reflections.getSubTypesOf(klass);

        Collection<PluginDesc<T>> result = new ArrayList<>();
        for (Class<? extends T> plugin : plugins) {
            if (isConcrete(plugin)) {
                // Temporary workaround until all the plugins are versioned.
                if (Connector.class.isAssignableFrom(plugin)) {
                    result.add(
                            new PluginDesc<>(
                                    plugin,
                                    ((Connector) plugin.newInstance()).version(),
                                    loader
                            )
                    );
                } else {
                    result.add(new PluginDesc<>(plugin, "undefined", loader));
                }
            }
        }
        return result;
    }

    public ClassLoader connectorLoader(Connector connector) {
        return connectorLoader(connector.getClass().getName());
    }

    public ClassLoader connectorLoader(String connectorClassOrAlias) {
        log.debug("Getting plugin class loader for connector: '{}'", connectorClassOrAlias);
        SortedMap<PluginDesc<?>, PluginClassLoader> inner =
                pluginLoaders.get(connectorClassOrAlias);
        if (inner == null) {
            log.error(
                    "Plugin class loader for connector: '{}' was not found. Returning: {}",
                    connectorClassOrAlias,
                    this
            );
            return this;
        }
        return inner.get(inner.lastKey());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!PluginUtils.shouldLoadInIsolation(name)) {
            // There are no paths in this classloader, will attempt to load with the parent.
            return super.loadClass(name, resolve);
        }

        SortedMap<PluginDesc<?>, PluginClassLoader> inner = pluginLoaders.get(name);
        if (inner != null) {
            log.warn("Class has been found before: {} by {}", name, inner.get(inner.lastKey()));
            return inner.get(inner.lastKey()).loadClass(name, resolve);
        }

        Class<?> klass = null;
        for (PluginClassLoader loader : activePaths.values()) {
            try {
                klass = loader.loadClass(name, resolve);
                break;
            } catch (ClassNotFoundException e) {
                // Not found in this loader.
            }
        }
        if (klass == null) {
            return super.loadClass(name, resolve);
        }
        return klass;
    }
}
