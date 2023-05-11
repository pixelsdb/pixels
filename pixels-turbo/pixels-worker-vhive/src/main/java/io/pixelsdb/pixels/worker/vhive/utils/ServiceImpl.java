package io.pixelsdb.pixels.worker.vhive.utils;

import com.alibaba.fastjson.JSON;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.worker.common.WorkerProto;

public class ServiceImpl <I, O> {
    final RequestHandler<I, O> handler;
    final Class<I> typeParameterClass;


    public ServiceImpl(
            RequestHandler<I, O> handler,
            Class<I> typeParameterClass) {
        this.handler = handler;
        this.typeParameterClass = typeParameterClass;
    }

    public void execute(WorkerProto.WorkerRequest request,
                        StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        I input = JSON.parseObject(request.getJson(), typeParameterClass);
        O output = handler.handleRequest(input);

        WorkerProto.WorkerResponse response = WorkerProto.WorkerResponse.newBuilder()
                .setJson(JSON.toJSONString(output))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
