/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.MockRestoreCallback;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class StateRestorerTest {

    private static final long OFFSET_LIMIT = 50;
    private final MockRestoreCallback callback = new MockRestoreCallback();
    private final StateRestorer restorer = new StateRestorer(new TopicPartition("topic", 1), callback, null, OFFSET_LIMIT);

    @Test
    public void shouldCallRestoreOnRestoreCallback() throws Exception {
        restorer.restore(new byte[0], new byte[0]);
        assertThat(callback.restoreCount, equalTo(1));
    }

    @Test
    public void shouldBeCompletedIfRecordOffsetGreaterThanEqualToEndOffset() throws Exception {
        assertTrue(restorer.hasCompleted(10, 10));
        assertTrue(restorer.hasCompleted(11, 10));
    }

    @Test
    public void shouldBeCompletedIfRecordOffsetGreaterThenEqualToOffsetLimit() throws Exception {
        assertTrue(restorer.hasCompleted(50, 100));
        assertTrue(restorer.hasCompleted(51, 100));
    }

    @Test
    public void shouldSetRestoredOffsetToMinOfLimitAndOffset() throws Exception {
        restorer.setRestoredOffset(20);
        assertThat(restorer.restoredOffset(), equalTo(20L));
        restorer.setRestoredOffset(100);
        assertThat(restorer.restoredOffset(), equalTo(OFFSET_LIMIT));
    }


}