package io.pixelsdb.pixels.worker.vhive.utils;

//public interface RequestHandler<I, O> {
//    O handleRequest(I var1, Context var2);
//}

import io.pixelsdb.pixels.common.turbo.WorkerType;

public interface RequestHandler<I, O>
{
    O handleRequest(I input);

    String getRequestId();

    WorkerType getWorkerType();
}
