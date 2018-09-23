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
package org.apache.kafka.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 */
public final class ApiUtils {
    private ApiUtils() {
    }

    /**
     * Validates that milliseconds from duration {@code d} can be retrieved.
     * @param d Duration to check
     * @param name Name of params for an error message.
     */
    public static void validateMillisecondDuration(final Duration d, final String name) {
        try {
            Objects.requireNonNull(d);

            //noinspection ResultOfMethodCallIgnored
            d.toMillis();
        } catch (final NullPointerException e) {
            throw new IllegalArgumentException(name + " shouldn't be null.", e);
        } catch (final ArithmeticException e) {
            throw new IllegalArgumentException(name + " can't be converted to milliseconds. " +  d +
                " is negative or too big", e);
        }
    }

    /**
     * Validates that milliseconds from instant {@code i} can be retrieved.
     * @param i Instant to check
     * @param name Name of params for an error message.
     */
    public static void validateMillisecondInstant(final Instant i, final String name) {
        try {
            Objects.requireNonNull(i);

            //noinspection ResultOfMethodCallIgnored
            i.toEpochMilli();
        } catch (final NullPointerException e) {
            throw new IllegalArgumentException(name + " shouldn't be null.", e);
        } catch (final ArithmeticException e) {
            throw new IllegalArgumentException(name + " can't be converted to milliseconds. " +  i +
                " is negative or too big", e);
        }
    }
}
