package io.pixelsdb.pixels.worker.vhive.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

public class ServiceImpl<T extends RequestHandler<I, O>, I extends Input, O extends Output> {
    private static final Logger log = LogManager.getLogger(ServiceImpl.class);

    final Class<T> handlerClass;
    final Class<I> typeParameterClass;

    public ServiceImpl(
            Class<T> handlerClass,
            Class<I> typeParameterClass) {
        this.handlerClass = handlerClass;
        this.typeParameterClass = typeParameterClass;
    }

    public void execute(TurboProto.WorkerRequest request,
                        StreamObserver<TurboProto.WorkerResponse> responseObserver) {
        I input = JSON.parseObject(request.getJson(), typeParameterClass);
        O output;

        boolean isProfile = Boolean.parseBoolean(System.getenv("PROFILING_ENABLED"));
        try {
            String requestId = String.valueOf(UUID.randomUUID());
            WorkerContext context = new WorkerContext(LogManager.getLogger(handlerClass), new WorkerMetrics(), requestId);
            RequestHandler<I, O> handler = handlerClass.getConstructor(WorkerContext.class).newInstance(context);

            String JSONFilename = String.format("%s_%s.json", handler.getWorkerType(), handler.getRequestId());
            if (isProfile) {
                log.info(String.format("enable profile to execute input: %s", JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect)));

                String JFRFilename = String.format("%s_%s.jfr", handler.getWorkerType(), handler.getRequestId());
                Utils.startProfile(JFRFilename);
                output = handler.handleRequest(input);
                Utils.stopProfile(JFRFilename);

                Utils.upload(JFRFilename, String.format("%s/%s", input.getTransId(), JFRFilename));
                log.info(String.format("upload JFR file to experiments/%s/%s successfully", input.getTransId(), JFRFilename));
            } else {
                log.info(String.format("disable profile to execute input: %s", JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect)));
                output = handler.handleRequest(input);
            }
            Utils.dump(JSONFilename, JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect));
            Utils.upload(JSONFilename, String.format("%s/%s", input.getTransId(), JSONFilename));
            log.info(String.format("upload JSON file to experiments/%s/%s successfully", input.getTransId(), JSONFilename));

            log.info(String.format("get output successfully: %s", JSON.toJSONString(output)));
        } catch (Exception e) {
            throw new RuntimeException("Exception during process: ", e);
        }
        TurboProto.WorkerResponse response = TurboProto.WorkerResponse.newBuilder()
                .setJson(JSON.toJSONString(output))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
