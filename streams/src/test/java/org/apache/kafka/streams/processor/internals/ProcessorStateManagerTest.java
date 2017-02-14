/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockChangelogReader;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ProcessorStateManagerTest {

    private final Set<TopicPartition> noPartitions = Collections.emptySet();
    private final String applicationId = "test-application";
    private final String persistentStoreName = "persistentStore";
    private final String nonPersistentStoreName = "nonPersistentStore";
    private final String persistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private final String nonPersistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);
    private final String storeName = "mockStateStore";
    private final TaskId taskId = new TaskId(0, 1);
    private final MockChangelogReader changelogReader = new MockChangelogReader();
    private final MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(storeName, true);
    private File baseDir;
    private StateDirectory stateDirectory;


    @Before
    public void setup() {
        baseDir = TestUtils.tempDirectory();
        stateDirectory = new StateDirectory(applicationId, baseDir.getPath(), new MockTime());
    }

    @After
    public void cleanup() {
        Utils.delete(baseDir);
    }

    @Test
    public void testRegisterPersistentStore() throws IOException {
        final TaskId taskId = new TaskId(0, 2);

        MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore("persistentStore", true); // persistent store
        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, noPartitions, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreName);
            }
        }, changelogReader);
        try {

            stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(persistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testRegisterNonPersistentStore() throws IOException {
        MockStateStoreSupplier.MockStateStore nonPersistentStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false); // non persistent store
        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 2), noPartitions, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreTopicName);
            }
        }, changelogReader);
        try {

            stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(nonPersistentStoreTopicName, 2)));

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testChangeLogOffsets() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        long lastCheckpointedOffset = 10L;
        String storeName1 = "store1";
        String storeName2 = "store2";
        String storeName3 = "store3";

        String storeTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
        String storeTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
        String storeTopicName3 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName3);

        Map<String, String> storeToChangelogTopic = new HashMap<>();
        storeToChangelogTopic.put(storeName1, storeTopicName1);
        storeToChangelogTopic.put(storeName2, storeTopicName2);
        storeToChangelogTopic.put(storeName3, storeTopicName3);

        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(new TopicPartition(storeTopicName1, 0), lastCheckpointedOffset));

        TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
        TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
        TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);

        MockStateStoreSupplier.MockStateStore store1 = new MockStateStoreSupplier.MockStateStore(storeName1, true);
        MockStateStoreSupplier.MockStateStore store2 = new MockStateStoreSupplier.MockStateStore(storeName2, true);
        MockStateStoreSupplier.MockStateStore store3 = new MockStateStoreSupplier.MockStateStore(storeName3, true);

        // if there is an source partition, inherit the partition id
        Set<TopicPartition> sourcePartitions = Utils.mkSet(new TopicPartition(storeTopicName3, 1));

        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, sourcePartitions, true, stateDirectory, storeToChangelogTopic, changelogReader); // standby
        try {
            stateMgr.register(store1, true, store1.stateRestoreCallback);
            stateMgr.register(store2, true, store2.stateRestoreCallback);
            stateMgr.register(store3, true, store3.stateRestoreCallback);

            Map<TopicPartition, Long> changeLogOffsets = stateMgr.checkpointedOffsets();

            assertEquals(3, changeLogOffsets.size());
            assertTrue(changeLogOffsets.containsKey(partition1));
            assertTrue(changeLogOffsets.containsKey(partition2));
            assertTrue(changeLogOffsets.containsKey(partition3));
            assertEquals(lastCheckpointedOffset, (long) changeLogOffsets.get(partition1));
            assertEquals(-1L, (long) changeLogOffsets.get(partition2));
            assertEquals(-1L, (long) changeLogOffsets.get(partition3));

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testGetStore() throws IOException {
        final MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 1), noPartitions, false, stateDirectory, Collections.<String, String>emptyMap(), changelogReader);
        try {
            stateMgr.register(mockStateStore, true, mockStateStore.stateRestoreCallback);

            assertNull(stateMgr.getStore("noSuchStore"));
            assertEquals(mockStateStore, stateMgr.getStore(nonPersistentStoreName));

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testFlushAndClose() throws IOException {
        final TaskId taskId = new TaskId(0, 1);
        final File checkpointFile = new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME);
        // write an empty checkpoint file
        final OffsetCheckpoint oldCheckpoint = new OffsetCheckpoint(checkpointFile);
        oldCheckpoint.write(Collections.<TopicPartition, Long>emptyMap());

        // set up ack'ed offsets
        final HashMap<TopicPartition, Long> ackedOffsets = new HashMap<>();
        ackedOffsets.put(new TopicPartition(persistentStoreTopicName, 1), 123L);
        ackedOffsets.put(new TopicPartition(nonPersistentStoreTopicName, 1), 456L);
        ackedOffsets.put(new TopicPartition(ProcessorStateManager.storeChangelogTopic(applicationId, "otherTopic"), 1), 789L);

        final MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore(persistentStoreName, true);
        final MockStateStoreSupplier.MockStateStore nonPersistentStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);

        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, noPartitions, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreTopicName);
            }
        }, changelogReader);
        try {
            // make sure the checkpoint file is deleted
            assertFalse(checkpointFile.exists());
            stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
            stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);
        } finally {
            // close the state manager with the ack'ed offsets
            stateMgr.flush(new MockProcessorContext(StateSerdes.withBuiltinTypes("foo", String.class, String.class), new NoOpRecordCollector()));
            stateMgr.close(ackedOffsets);
        }

        // make sure all stores are closed, and the checkpoint file is written.
        assertTrue(persistentStore.flushed);
        assertTrue(persistentStore.closed);
        assertTrue(nonPersistentStore.flushed);
        assertTrue(nonPersistentStore.closed);
        assertTrue(checkpointFile.exists());

        // the checkpoint file should contain an offset from the persistent store only.
        OffsetCheckpoint newCheckpoint = new OffsetCheckpoint(checkpointFile);
        Map<TopicPartition, Long> checkpointedOffsets = newCheckpoint.read();
        assertEquals(1, checkpointedOffsets.size());
        assertEquals(new Long(124), checkpointedOffsets.get(new TopicPartition(persistentStoreTopicName, 1)));
    }

    @Test
    public void shouldRegisterStoreWithoutLoggingEnabledAndNotBackedByATopic() throws Exception {
        MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);
        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 1), noPartitions, false, stateDirectory, Collections.<String, String>emptyMap(), changelogReader);
        stateMgr.register(mockStateStore, false, mockStateStore.stateRestoreCallback);
        assertNotNull(stateMgr.getStore(nonPersistentStoreName));
    }

    @Test
    public void shouldNotWriteCheckpointsIfAckeOffsetsIsNull() throws Exception {
        final File checkpointFile = new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME);
        // write an empty checkpoint file
        final OffsetCheckpoint oldCheckpoint = new OffsetCheckpoint(checkpointFile);
        oldCheckpoint.write(Collections.<TopicPartition, Long>emptyMap());

        final MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, noPartitions, false, stateDirectory, Collections.<String, String>emptyMap(), changelogReader);

        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
        stateMgr.close(null);
        assertFalse(checkpointFile.exists());
    }

    @Test
    public void shouldThrowLockExceptionIfFailedToLockStateDirectory() throws Exception {
        final File taskDirectory = stateDirectory.directoryForTask(taskId);
        final FileChannel channel = FileChannel.open(new File(taskDirectory,
                                                              StateDirectory.LOCK_FILE_NAME).toPath(),
                                                     StandardOpenOption.CREATE,
                                                     StandardOpenOption.WRITE);
        // lock the task directory
        final FileLock lock = channel.lock();

        try {
            new ProcessorStateManager(taskId, noPartitions, false, stateDirectory, Collections.<String, String>emptyMap(), changelogReader);
            fail("Should have thrown LockException");
        } catch (final LockException e) {
           // pass
        } finally {
            lock.release();
            channel.close();
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName() throws Exception {
        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             noPartitions,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.<String, String>emptyMap(), changelogReader);

        try {
            stateManager.register(new MockStateStoreSupplier.MockStateStore(ProcessorStateManager.CHECKPOINT_FILE_NAME, true), true, null);
            fail("should have thrown illegal argument exception when store name same as checkpoint file");
        } catch (final IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionOnRegisterWhenStoreHasAlreadyBeenRegistered() throws Exception {
        final ProcessorStateManager stateManager = new ProcessorStateManager(taskId,
                                                                             noPartitions,
                                                                             false,
                                                                             stateDirectory,
                                                                             Collections.<String, String>emptyMap(), changelogReader);
        stateManager.register(mockStateStore, false, null);

        try {
            stateManager.register(mockStateStore, false, null);
            fail("should have thrown illegal argument exception when store with same name already registered");
        } catch (final IllegalArgumentException e) {
            // pass
        }
        
    }


}
