package org.apache.kafka.storage.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A remote log offset read task future holder. It contains two futures:
 * 1. JobFuture - Use this future to cancel the running job.
 * 2. TaskFuture - Use this future to get the result of the job/computation.
 */
public record AsyncOffsetReadFutureHolder<T>(
        Future<Void> jobFuture,
        CompletableFuture<T> taskFuture
) {
}
