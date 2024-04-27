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

package kafka.test.junit;

import kafka.test.annotation.ClusterTemplate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import java.util.function.Consumer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterTestExtensionsUnitTest {
    @Test
    void testProcessClusterTemplate() {
        ClusterTestExtensions ext = new ClusterTestExtensions();
        ExtensionContext context = mock(ExtensionContext.class);
        Consumer<TestTemplateInvocationContext> testInvocations = mock(Consumer.class);
        ClusterTemplate annot = mock(ClusterTemplate.class);
        when(annot.value()).thenReturn("");

        Assertions.assertThrows(IllegalStateException.class, () -> {
                ext.processClusterTemplate(context, annot, testInvocations);
            }
        );
    }
}
