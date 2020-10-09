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

package org.apache.kafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class LocalQuorumsTestEnv implements AutoCloseable {
    private static final Logger log =
        LoggerFactory.getLogger(LocalQuorumsTestEnv.class);

    private final LocalLogManagerTestEnv logManagerTestEnv;

    private final List<QuorumController> controllers;

    public LocalQuorumsTestEnv(int numControllers,
            Consumer<QuorumController.Builder> builderCallback) throws Exception {
        this.logManagerTestEnv = new LocalLogManagerTestEnv(numControllers);
        this.controllers = new ArrayList<>();
        try {
            for (int i = 0; i < numControllers; i++) {
                QuorumController.Builder builder = new QuorumController.Builder(i);
                builderCallback.accept(builder);
                builder.setLogManager(logManagerTestEnv.logManagers().get(i));
                controllers.add(builder.build());
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    @Override
    public void close() throws InterruptedException {
        logManagerTestEnv.close();
        for (QuorumController controller : controllers) {
            controller.close();
        }
    }
}
