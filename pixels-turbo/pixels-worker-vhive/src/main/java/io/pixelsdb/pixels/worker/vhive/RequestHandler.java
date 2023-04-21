package io.pixelsdb.pixels.worker.vhive;

//public interface RequestHandler<I, O> {
//    O handleRequest(I var1, Context var2);
//}

public interface RequestHandler<I, O> {
    O handleRequest(I input);
}
