package io.pixelsdb.pixels.worker.vhive.utils;

//public interface RequestHandler<I, O> {
//    O handleRequest(I var1, Context var2);
//}

public interface RequestHandler<I, O> {
    O handleRequest(I input);
}
