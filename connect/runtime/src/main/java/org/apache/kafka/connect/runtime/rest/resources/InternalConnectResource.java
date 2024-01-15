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
package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;

import javax.inject.Inject;
import javax.ws.rs.Path;

@Path("/connectors")
public class InternalConnectResource extends InternalClusterResource {

    private final Herder herder;

    @Inject
    public InternalConnectResource(Herder herder, RestClient restClient, RestRequestTimeout requestTimeout) {
        super(restClient, requestTimeout);
        this.herder = herder;
    }

    @Override
    protected Herder herderForRequest() {
        return herder;
    }

}
