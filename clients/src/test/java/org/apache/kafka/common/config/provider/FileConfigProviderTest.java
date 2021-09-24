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
package org.apache.kafka.common.config.provider;

import org.apache.kafka.common.config.ConfigData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileConfigProviderTest {

    private FileConfigProvider configProvider;

    @BeforeEach
    public void setup() {
        configProvider = new TestFileConfigProvider();
    }

    @Test
    public void testGetAllKeysAtPath() throws Exception {
        ConfigData configData = configProvider.get("dummy");
        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        result.put("testKey2", "testResult2");
        assertEquals(result, configData.data());
        assertNull(configData.ttl());
    }

    @Test
    public void testGetOneKeyAtPath() throws Exception {
        ConfigData configData = configProvider.get("dummy", Collections.singleton("testKey"));
        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        assertEquals(result, configData.data());
        assertNull(configData.ttl());
    }

    @Test
    public void testEmptyPath() throws Exception {
        ConfigData configData = configProvider.get("", Collections.singleton("testKey"));
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testEmptyPathWithKey() throws Exception {
        ConfigData configData = configProvider.get("");
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNullPath() throws Exception {
        ConfigData configData = configProvider.get(null);
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNullPathWithKey() throws Exception {
        ConfigData configData = configProvider.get(null, Collections.singleton("testKey"));
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testServiceLoaderDiscovery() {
        ServiceLoader<ConfigProvider> serviceLoader = ServiceLoader.load(ConfigProvider.class);

        boolean discovered = false;

        for (ConfigProvider service : serviceLoader)    {
            System.out.println(service.getClass());
            if (service instanceof FileConfigProvider) {
                discovered = true;
            }
        }

        assertTrue(discovered);
    }

    public static class TestFileConfigProvider extends FileConfigProvider {

        @Override
        protected Reader reader(String path) throws IOException {
            return new StringReader("testKey=testResult\ntestKey2=testResult2");
        }
    }
}
