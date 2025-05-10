package io.pixelsdb.pixels.worker.spike;

import io.pixelsdb.pixels.common.turbo.WorkerType;

public interface WorkerInterface<I, O> {
    O handleRequest(I input);

    String getRequestId();

    WorkerType getWorkerType();
}