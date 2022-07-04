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

package org.apache.kafka.connect.runtime.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.jose4j.keys.HmacKey;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestClientTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<TestDTO> TEST_TYPE = new TypeReference<TestDTO>() {
    };

    private final HttpClient httpClient = mock(HttpClient.class);

    private static String toJsonString(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static RestClient.HttpResponse<TestDTO> httpRequest(HttpClient httpClient, String requestSignatureAlgorithm) {
        return RestClient.httpRequest(
                httpClient,
                "https://localhost:1234/api/endpoint",
                "GET",
                null,
                new TestDTO("requestBodyData"),
                TEST_TYPE,
                new HmacKey("HMAC".getBytes(StandardCharsets.UTF_8)),
                requestSignatureAlgorithm);
    }

    private static RestClient.HttpResponse<TestDTO> httpRequest(HttpClient httpClient) {
        String validRequestSignatureAlgorithm = "HmacMD5";
        return RestClient.httpRequest(
                httpClient,
                "https://localhost:1234/api/endpoint",
                "GET",
                null,
                new TestDTO("requestBodyData"),
                TEST_TYPE,
                new HmacKey("HMAC".getBytes(StandardCharsets.UTF_8)),
                validRequestSignatureAlgorithm);

    }

    private static void assertIsInternalServerError(ConnectRestException e) {
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.statusCode());
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.errorCode());
    }

    @Test
    public void testSuccess() throws Exception {
        int statusCode = Response.Status.OK.getStatusCode();
        TestDTO expectedResponse = new TestDTO("someContent");
        setupHttpClient(statusCode, toJsonString(expectedResponse));

        RestClient.HttpResponse<TestDTO> httpResp = httpRequest(httpClient);
        assertEquals(statusCode, httpResp.status());
        assertEquals(expectedResponse, httpResp.body());
    }

    @Test
    public void testNoContent() throws Exception {
        int statusCode = Response.Status.NO_CONTENT.getStatusCode();
        setupHttpClient(statusCode, null);

        RestClient.HttpResponse<TestDTO> httpResp = httpRequest(httpClient);
        assertEquals(statusCode, httpResp.status());
        assertNull(httpResp.body());
    }

    @Test
    public void testStatusCodeAndErrorMessagePreserved() throws Exception {
        int statusCode = Response.Status.CONFLICT.getStatusCode();
        ErrorMessage errorMsg = new ErrorMessage(Response.Status.GONE.getStatusCode(), "Some Error Message");
        setupHttpClient(statusCode, toJsonString(errorMsg));
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient));
        assertEquals(statusCode, e.statusCode());
        assertEquals(errorMsg.errorCode(), e.errorCode());
        assertEquals(errorMsg.message(), e.getMessage());
    }

    @Test
    public void testUnexpectedHttpResponseCausesInternalServerError() throws Exception {
        int statusCode = Response.Status.NOT_MODIFIED.getStatusCode(); // never thrown explicitly -
        // should be treated as an unexpected error and translated into 500 INTERNAL_SERVER_ERROR

        setupHttpClient(statusCode, null);
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient));
        assertIsInternalServerError(e);
    }

    @Test
    public void testRuntimeExceptionCausesInternalServerError() {
        when(httpClient.newRequest(anyString())).thenThrow(new RuntimeException());

        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient));
        assertIsInternalServerError(e);
    }

    @Test
    public void testRequestSignatureFailureCausesInternalServerError() throws Exception {
        setupHttpClient(0, null);
        String invalidRequestSignatureAlgorithm = "Foo";
        ConnectRestException e = assertThrows(ConnectRestException.class, () -> httpRequest(httpClient, invalidRequestSignatureAlgorithm));
        assertIsInternalServerError(e);
    }

    private void setupHttpClient(int responseCode, String responseJsonString) throws Exception {
        Request req = mock(Request.class);
        ContentResponse resp = mock(ContentResponse.class);
        when(resp.getStatus()).thenReturn(responseCode);
        when(resp.getContentAsString()).thenReturn(responseJsonString);
        when(req.send()).thenReturn(resp);
        when(req.header(anyString(), anyString())).thenReturn(req);
        when(httpClient.newRequest(anyString())).thenReturn(req);
    }

    private static class TestDTO {
        private final String content;

        @JsonCreator
        private TestDTO(@JsonProperty(value = "content") String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestDTO testDTO = (TestDTO) o;
            return content.equals(testDTO.content);
        }

        @Override
        public int hashCode() {
            return Objects.hash(content);
        }
    }
}
