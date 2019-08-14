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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.easymock.Capture;
import org.eclipse.jetty.client.api.Request;
import org.junit.Test;

import javax.crypto.Mac;
import javax.ws.rs.core.HttpHeaders;

import java.util.Base64;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class InternalRequestSignatureTest {

    private static final byte[] REQUEST_BODY =
            "[{\"config\":\"value\"},{\"config\":\"other_value\"}]".getBytes();
    private static final String SIGNATURE_ALGORITHM = "HmacSHA256";
    private static final byte[] KEY = new byte[] {
        109, 116, -111, 49, -94, 25, -103, 44, -99, -118, 53, -69, 87, -124, 5, 48,
        89, -105, -2, 58, -92, 87, 67, 49, -125, -79, -39, -126, -51, -53, -85, 57
    };
    private static final byte[] SIGNATURE = new byte[] {
        42, -3, 127, 57, 43, 49, -51, -43, 72, -62, -10, 120, 123, 125, 26, -65,
        36, 72, 86, -71, -32, 13, -8, 115, 85, 73, -65, -112, 6, 68, 41, -50
    };
    private static final String ENCODED_SIGNATURE = Base64.getEncoder().encodeToString(SIGNATURE);

    @Test
    public void fromHeadersShouldReturnNullOnNullHeaders() {
        assertNull(InternalRequestSignature.fromHeaders(REQUEST_BODY, null));
    }

    @Test
    public void fromHeadersShouldReturnNullIfSignatureHeaderMissing() {
        assertNull(InternalRequestSignature.fromHeaders(REQUEST_BODY, internalRequestHeaders(null, SIGNATURE_ALGORITHM)));
    }

    @Test
    public void fromHeadersShouldReturnNullIfSignatureAlgorithmHeaderMissing() {
        assertNull(InternalRequestSignature.fromHeaders(REQUEST_BODY, internalRequestHeaders(ENCODED_SIGNATURE, null)));
    }

    @Test(expected = BadRequestException.class)
    public void fromHeadersShouldThrowExceptionOnInvalidSignatureAlgorithm() {
        InternalRequestSignature.fromHeaders(REQUEST_BODY, internalRequestHeaders(ENCODED_SIGNATURE, "doesn'texist"));
    }

    @Test
    public void fromHeadersShouldReturnNonNullResultOnValidSignatureAndSignatureAlgorithm() {
        InternalRequestSignature signature =
                InternalRequestSignature.fromHeaders(REQUEST_BODY, internalRequestHeaders(ENCODED_SIGNATURE, SIGNATURE_ALGORITHM));
        assertNotNull(signature);
        assertNotNull(signature.keyAlgorithm());
    }

    @Test(expected = ConnectException.class)
    public void addToRequestShouldThrowExceptionOnInvalidSignatureAlgorithm() {
        Request request = mock(Request.class);
        replay(request);
        InternalRequestSignature.addToRequest(KEY, REQUEST_BODY, "doesn'texist", request);
    }

    @Test
    public void addToRequestShouldAddHeadersOnValidSignatureAlgorithm() {
        Request request = mock(Request.class);
        Capture<String> signatureCapture = newCapture();
        Capture<String> signatureAlgorithmCapture = newCapture();
        expect(request.header(eq(InternalRequestSignature.SIGNATURE_HEADER), capture(signatureCapture)))
            .andReturn(request)
            .once();
        expect(request.header(eq(InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER), capture(signatureAlgorithmCapture)))
            .andReturn(request)
            .once();

        replay(request);
        InternalRequestSignature.addToRequest(KEY, REQUEST_BODY, SIGNATURE_ALGORITHM, request);

        assertEquals(
            "Request should have valid base 64-encoded signature added as header",
            ENCODED_SIGNATURE,
            signatureCapture.getValue()
        );
        assertEquals(
            "Request should have provided signature algorithm added as header",
            SIGNATURE_ALGORITHM,
            signatureAlgorithmCapture.getValue()
        );
    }

    @Test
    public void testSignatureValidation() throws Exception {
        Mac mac = Mac.getInstance(SIGNATURE_ALGORITHM);

        InternalRequestSignature signature = new InternalRequestSignature(REQUEST_BODY, mac, SIGNATURE);
        assertTrue(signature.isValid(KEY));

        signature = InternalRequestSignature.fromHeaders(REQUEST_BODY, internalRequestHeaders(ENCODED_SIGNATURE, SIGNATURE_ALGORITHM));
        assertTrue(signature.isValid(KEY));

        signature = new InternalRequestSignature("[{\"different_config\":\"different_value\"}]".getBytes(), mac, SIGNATURE);
        assertFalse(signature.isValid(KEY));

        signature = new InternalRequestSignature(REQUEST_BODY, mac, "bad signature".getBytes());
        assertFalse(signature.isValid(KEY));
    }

    private static HttpHeaders internalRequestHeaders(String signature, String signatureAlgorithm) {
        HttpHeaders result = mock(HttpHeaders.class);
        expect(result.getHeaderString(eq(InternalRequestSignature.SIGNATURE_HEADER)))
            .andReturn(signature);
        expect(result.getHeaderString(eq(InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER)))
            .andReturn(signatureAlgorithm);
        replay(result);
        return result;
    }
}
