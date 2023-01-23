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
package org.apache.kafka.server.util;

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


public class FutureUtils {
    /**
     * Based on the current time and a delay, computes a monotonic deadline in the future.
     *
     * @param nowNs     The current time in monotonic nanoseconds.
     * @param delayMs   The delay in milliseconds.
     * @return          The monotonic deadline in the future. This value is capped at
     *                  Long.MAX_VALUE.
     */
    public static long getDeadlineNsFromDelayMs(
        long nowNs,
        long delayMs
    ) {
        if (delayMs < 0) {
            throw new RuntimeException("Negative delays are not allowed.");
        }
        BigInteger delayNs = BigInteger.valueOf(delayMs).multiply(BigInteger.valueOf(1_000_000));
        BigInteger deadlineNs = BigInteger.valueOf(nowNs).add(delayNs);
        if (deadlineNs.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) >= 0) {
            return Long.MAX_VALUE;
        } else {
            return deadlineNs.longValue();
        }
    }

    /**
     * Wait for a future until a specific time in the future, with copious logging.
     *
     * @param log           The slf4j object to use to log success and failure.
     * @param action        The action we are waiting for.
     * @param future        The future we are waiting for.
     * @param deadlineNs    The deadline in the future we are waiting for.
     * @param time          The clock object.
     * @return              The result of the future.
     * @param <T>           The type of the future.
     *
     * @throws java.util.concurrent.TimeoutException If the future times out.
     * @throws Throwable If the future fails. Note: we unwrap ExecutionException here.
     */
    public static <T> T waitWithLogging(
        Logger log,
        String action,
        CompletableFuture<T> future,
        long deadlineNs,
        Time time
    ) throws Throwable {
        log.info("Waiting for {}", action);
        try {
            T result = time.waitForFuture(future, deadlineNs);
            log.info("Finished waiting for {}", action);
            return result;
        } catch (TimeoutException t)  {
            log.error("Timed out while waiting for {}", action, t);
            TimeoutException timeout = new TimeoutException("Timed out while waiting for " + action);
            timeout.setStackTrace(t.getStackTrace());
            throw timeout;
        } catch (Throwable t)  {
            if (t instanceof ExecutionException) {
                ExecutionException executionException = (ExecutionException) t;
                t = executionException.getCause();
            }
            log.error("Received a fatal error while waiting for {}", action, t);
            throw new RuntimeException("Received a fatal error while waiting for " + action, t);
        }
    }
}
