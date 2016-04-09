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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.io.PrintStream;


class KeyValuePrinter<K, V> implements ProcessorSupplier<K, V> {

    private final PrintStream printStream;
    private Serde<?> keySerde;
    private Serde<?> valueSerde;
    private static final String NULL_KEY = "null_key";
    private boolean notStandardOut;


    KeyValuePrinter(PrintStream printStream, Serde<?> keySerde, Serde<?> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        if (printStream == null) {
            this.printStream = System.out;
        } else {
            this.printStream = printStream;
            notStandardOut = true;
        }
    }

    KeyValuePrinter(PrintStream printStream) {
        this(printStream, null, null);
    }

    KeyValuePrinter(Serde<?> keySerde, Serde<?> valueSerde) {
        this(null, keySerde, valueSerde);
    }

    KeyValuePrinter() {
        this(null, null, null);
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamPrinterProcessor(this.printStream, this.keySerde, this.valueSerde);
    }


    private class KStreamPrinterProcessor extends AbstractProcessor<K, V> {
        private final PrintStream printStream;
        private Serde<?> keySerde;
        private Serde<?> valueSerde;
        private ProcessorContext processorContext;

        private KStreamPrinterProcessor(PrintStream printStream, Serde<?> keySerde, Serde<?> valueSerde) {
            this.printStream = printStream;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        @Override
        public void init(ProcessorContext context) {
            this.processorContext = context;

            if (this.keySerde == null) {
                keySerde = this.processorContext.keySerde();
            }

            if (this.valueSerde == null) {
                valueSerde = this.processorContext.valueSerde();
            }
        }

        @Override
        public void process(K key, V value) {
            Object keyToPrint = maybeDeserialize(key, keySerde.deserializer());
            Object valueToPrint = maybeDeserialize(value, valueSerde.deserializer());

            String stringKey = keyToPrint == null ? NULL_KEY : keyToPrint.toString();

            printStream.println(stringKey + " , " + valueToPrint);

            this.processorContext.forward(key, value);
        }


        private Object maybeDeserialize(Object receivedElement, Deserializer<?> deserializer) {
            if (receivedElement == null) {
                return null;
            }

            if (receivedElement instanceof byte[]) {
                //TODO  calling this.processorContext.topic() throws Exception in unit test
                return deserializer.deserialize("", (byte[]) receivedElement);
            }

            return receivedElement;
        }

        @Override
        public void close() {
            if (notStandardOut) {
                this.printStream.close();
            }
        }
    }
}
