/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security;

import java.io.File;
import java.net.URI;
import java.security.URIParameter;
import javax.security.auth.login.Configuration;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaasUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JaasUtils.class);
    public static final String LOGIN_CONTEXT_SERVER = "KafkaServer";
    public static final String LOGIN_CONTEXT_CLIENT = "KafkaClient";
    public static final String SERVICE_NAME = "serviceName";
    public static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";

    public static boolean isSecure(String loginConfigFile) {
        boolean isSecurityEnabled = false;
        if (loginConfigFile != null && loginConfigFile.length() > 0) {
            File configFile = new File(loginConfigFile);
            if (!configFile.canRead()) {
                throw new KafkaException("File " + loginConfigFile + "cannot be read.");
            }
            try {
                URI configUri = configFile.toURI();
                Configuration loginConf = Configuration.getInstance("JavaLoginConfig", new URIParameter(configUri));
                isSecurityEnabled = loginConf.getAppConfigurationEntry("Client") != null;
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }

        return isSecurityEnabled;
    }
}