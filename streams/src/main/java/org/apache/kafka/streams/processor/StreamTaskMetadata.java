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
package org.apache.kafka.streams.processor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StreamTaskMetadata extends TaskId {
    private final int numberOfPartitions;
    private final int numberOfStateStores;
    
    public StreamTaskMetadata(final TaskId taskId,
                              final int numberOfPartitions, 
                              final int numberOfStateStores) { 
        super(taskId.topicGroupId, taskId.partition);
        this.numberOfPartitions = numberOfPartitions;
        this.numberOfStateStores = numberOfStateStores;
    }

    public int numberOfPartitions() {
        return numberOfPartitions;
    }

    public int numberOfStateStores() {
        return numberOfStateStores;
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putInt(topicGroupId);
        buffer.putInt(partition);
        buffer.putInt(numberOfPartitions);
        buffer.putInt(numberOfStateStores);
    }

    public static StreamTaskMetadata readFrom(final ByteBuffer buffer) {
        return new StreamTaskMetadata(TaskId.readFrom(buffer), 
                                      buffer.getInt(), 
                                      buffer.getInt());
    }

    @Override
    public void writeTo(final DataOutputStream stream) throws IOException {
        stream.writeInt(numberOfPartitions);
        stream.writeInt(numberOfStateStores);
    }

    public static StreamTaskMetadata readFrom(final DataInputStream stream) throws IOException {
        return new StreamTaskMetadata(TaskId.readFrom(stream), 
                                      stream.readInt(), 
                                      stream.readInt());
    }
}
