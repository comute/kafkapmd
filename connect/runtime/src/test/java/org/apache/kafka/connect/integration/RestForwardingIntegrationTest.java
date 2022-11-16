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
package org.apache.kafka.connect.integration;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.distributed.RequestTargetException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.util.SSLUtils;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@SuppressWarnings("unchecked")
@Category(IntegrationTest.class)
public class RestForwardingIntegrationTest {

    @Mock
    private Plugins plugins;
    private RestClient followerClient;
    private RestServer followerServer;
    @Mock
    private Herder followerHerder;
    private RestClient leaderClient;
    private RestServer leaderServer;
    @Mock
    private Herder leaderHerder;

    private SslContextFactory factory;
    private CloseableHttpClient httpClient;
    private Collection<CloseableHttpResponse> responses = new ArrayList<>();

    @After
    public void tearDown() throws IOException {
        for (CloseableHttpResponse response: responses) {
            response.close();
        }
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        Utils.closeAllQuietly(
                firstException,
                "clientsAndServers",
                httpClient,
                followerClient,
                followerServer != null ? followerServer::stop : null,
                leaderClient,
                leaderServer != null ? leaderServer::stop : null,
                factory != null ? factory::stop : null
        );
        if (firstException.get() != null) {
            throw new RuntimeException("Unable to cleanly close resources", firstException.get());
        }
    }

    @Test
    public void testRestForwardNoSsl() throws Exception {
        testRestForwardToLeader(false, false);
    }

    @Test
    public void testRestForwardSsl() throws Exception {
        testRestForwardToLeader(true, true);
    }

    @Test
    public void testRestForwardLeaderSsl() throws Exception {
        testRestForwardToLeader(false, true);
    }

    @Test
    public void testRestForwardFollowerSsl() throws Exception {
        testRestForwardToLeader(true, false);
    }

    public void testRestForwardToLeader(boolean followerSsl, boolean leaderSsl) throws Exception {
        DistributedConfig followerConfig = new DistributedConfig(baseWorkerProps(followerSsl));
        DistributedConfig leaderConfig = new DistributedConfig(baseWorkerProps(leaderSsl));

        // Follower worker setup
        followerClient = new RestClient(followerConfig);
        followerServer = new RestServer(followerConfig, followerClient);
        followerServer.initializeServer();
        when(followerHerder.plugins()).thenReturn(plugins);
        followerServer.initializeResources(followerHerder);

        // Leader worker setup
        leaderClient = new RestClient(leaderConfig);
        leaderServer = new RestServer(leaderConfig, leaderClient);
        leaderServer.initializeServer();
        when(leaderHerder.plugins()).thenReturn(plugins);
        leaderServer.initializeResources(leaderHerder);

        // External client setup
        factory = SSLUtils.createClientSideSslContextFactory(followerConfig);
        factory.start();
        SSLContext ssl = factory.getSslContext();
        httpClient = HttpClients.custom()
                .setSSLContext(ssl)
                .build();

        // Follower will forward to the leader
        URI leaderUrl = leaderServer.advertisedUrl();
        RequestTargetException forwardException = new NotLeaderException("Not leader", leaderUrl.toString());
        ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> followerCallbackCaptor = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            followerCallbackCaptor.getValue().onCompletion(forwardException, null);
            return null;
        }).when(followerHerder)
                .putConnectorConfig(any(), any(), anyBoolean(), followerCallbackCaptor.capture());

        // Leader will reply
        Herder.Created<ConnectorInfo> leaderAnswer = new Herder.Created<>(true, null);
        ArgumentCaptor<Callback<Herder.Created<ConnectorInfo>>> leaderCallbackCaptor = ArgumentCaptor.forClass(Callback.class);
        doAnswer(invocation -> {
            leaderCallbackCaptor.getValue().onCompletion(null, leaderAnswer);
            return null;
        }).when(followerHerder)
                .putConnectorConfig(any(), any(), anyBoolean(), leaderCallbackCaptor.capture());

        // Client makes request to the follower
        URI followerUrl = followerServer.advertisedUrl();
        HttpPost request = new HttpPost("/connectors");
        String jsonBody = "{" +
                "\"name\": \"blah\"," +
                "\"config\": {}" +
                "}";
        StringEntity entity = new StringEntity(jsonBody, StandardCharsets.UTF_8.name());
        entity.setContentType("application/json");
        request.setEntity(entity);
        HttpResponse httpResponse = executeRequest(followerUrl, request);

        // And sees the success from the leader
        assertEquals(201, httpResponse.getStatusLine().getStatusCode());
    }

    private Map<String, String> baseWorkerProps(boolean ssl) throws IOException, GeneralSecurityException {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "config-topic");
        workerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "connect-test-group");
        workerProps.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        if (ssl) {
            Map<String, Object> sslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, TestUtils.tempFile(), "testCert");
            for (String k : sslConfig.keySet()) {
                if (sslConfig.get(k) instanceof Password) {
                    workerProps.put(k, ((Password) sslConfig.get(k)).value());
                } else if (sslConfig.get(k) instanceof List) {
                    workerProps.put(k, String.join(",", (List<String>) sslConfig.get(k)));
                } else {
                    workerProps.put(k, sslConfig.get(k).toString());
                }
            }
            workerProps.put(WorkerConfig.LISTENERS_CONFIG, "HTTPS://localhost:0");
        } else {
            workerProps.put(WorkerConfig.LISTENERS_CONFIG, "HTTP://localhost:0");
        }

        return workerProps;
    }

    private HttpResponse executeRequest(URI serverUrl, HttpRequest request) throws IOException {
        HttpHost httpHost = new HttpHost(serverUrl.getHost(), serverUrl.getPort(), serverUrl.getScheme());
        CloseableHttpResponse response = httpClient.execute(httpHost, request);
        responses.add(response);
        return response;
    }
}
