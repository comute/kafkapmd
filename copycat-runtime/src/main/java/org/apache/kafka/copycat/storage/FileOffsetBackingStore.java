/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.copycat.errors.CopycatRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Implementation of OffsetBackingStore that saves data locally to a file. To ensure this behaves
 * similarly to a real backing store, operations are executed asynchronously on a background thread.
 */
public class FileOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(FileOffsetBackingStore.class);

    public final static String OFFSET_STORAGE_FILE_FILENAME_CONFIG = "offset.storage.file.filename";
    private File file;

    public FileOffsetBackingStore() {

    }

    @Override
    public void configure(Properties props) {
        super.configure(props);
        String filename = props.getProperty(OFFSET_STORAGE_FILE_FILENAME_CONFIG);
        file = new File(filename);
    }

    @Override
    public synchronized void start() {
        super.start();
        log.info("Starting FileOffsetBackingStore with file {}", file);
        load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        // Nothing to do since this doesn't maintain any outstanding connections/data
        log.info("Stopped FileOffsetBackingStore");
    }

    private void load() {
        try {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(file));
            HashMap<String, Map<byte[], byte[]>> raw
                    = (HashMap<String, Map<byte[], byte[]>>) is.readObject();
            data = new HashMap<String, Map<ByteBuffer, ByteBuffer>>();
            for (Map.Entry<String, Map<byte[], byte[]>> entry : raw.entrySet()) {
                HashMap<ByteBuffer, ByteBuffer> converted = new HashMap<ByteBuffer, ByteBuffer>();
                for (Map.Entry<byte[], byte[]> mapEntry : entry.getValue().entrySet()) {
                    ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                    ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) :
                            null;
                    converted.put(key, value);
                }
                data.put(entry.getKey(), converted);
            }
            is.close();
        } catch (FileNotFoundException e) {
            // Ignore, may be new
        } catch (EOFException e) {
            // Ignore, this means the file was missing or corrupt
        } catch (IOException e) {
            throw new CopycatRuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new CopycatRuntimeException(e);
        }
    }

    protected void save() {
        try {
            ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(file));
            HashMap<String, Map<byte[], byte[]>> raw = new HashMap<String, Map<byte[], byte[]>>();
            for (Map.Entry<String, Map<ByteBuffer, ByteBuffer>> entry : data.entrySet()) {
                HashMap<byte[], byte[]> converted = new HashMap<byte[], byte[]>();
                for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : entry.getValue().entrySet()) {
                    byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
                    byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
                    converted.put(key, value);
                }
                raw.put(entry.getKey(), converted);
            }
            os.writeObject(raw);
            os.close();
        } catch (IOException e) {
            throw new CopycatRuntimeException(e);
        }
    }
}
