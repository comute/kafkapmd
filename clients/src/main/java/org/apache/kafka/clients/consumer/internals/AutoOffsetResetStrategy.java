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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

public class AutoOffsetResetStrategy {
    private enum StrategyType {
        LATEST, EARLIEST, NONE;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public static final AutoOffsetResetStrategy EARLIEST = new AutoOffsetResetStrategy(StrategyType.EARLIEST);
    public static final AutoOffsetResetStrategy LATEST = new AutoOffsetResetStrategy(StrategyType.LATEST);
    public static final AutoOffsetResetStrategy NONE = new AutoOffsetResetStrategy(StrategyType.NONE);

    private final StrategyType type;

    private AutoOffsetResetStrategy(StrategyType type) {
        this.type = type;
    }

    public static boolean isValid(String offsetStrategy) {
        return Arrays.asList(Utils.enumOptions(StrategyType.class)).contains(offsetStrategy);
    }

    public static AutoOffsetResetStrategy fromString(String offsetStrategy) {
        if (offsetStrategy == null || !isValid(offsetStrategy)) {
            throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
        }
        StrategyType type = StrategyType.valueOf(offsetStrategy.toUpperCase(Locale.ROOT));
        switch (type) {
            case EARLIEST:
                return EARLIEST;
            case LATEST:
                return LATEST;
            case NONE:
                return NONE;
            default:
                throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
        }
    }

    /**
     * Returns the name of the offset reset strategy.
     */
    public String name() {
        return type.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoOffsetResetStrategy that = (AutoOffsetResetStrategy) o;
        return Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type);
    }

    @Override
    public String toString() {
        return "AutoOffsetResetStrategy{" +
                "type='" + type + '\'' +
                '}';
    }

    public static class Validator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String strategy = (String) value;
            if (!AutoOffsetResetStrategy.isValid(strategy)) {
                throw new ConfigException(name, value, "Invalid value " + strategy + " for configuration " +
                        name + ": the value must be either 'earliest', 'latest', or 'none'.");
            }
        }
    }
}
