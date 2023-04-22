package io.pixelsdb.pixels.worker.vhive.utils;

public interface Converter<T, U>{
    U physicalToProto(T request);
    T protoToPhysical(U request);
}
