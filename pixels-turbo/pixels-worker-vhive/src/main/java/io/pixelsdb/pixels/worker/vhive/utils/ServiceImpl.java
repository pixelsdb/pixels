package io.pixelsdb.pixels.worker.vhive.utils;

import com.alibaba.fastjson.JSON;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import one.profiler.AsyncProfiler;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.util.UUID;

public class ServiceImpl<T extends RequestHandler<I, O>, I extends Input, O extends Output> {
    private static final Logger log = LogManager.getLogger(ServiceImpl.class);
    private static final AsyncProfiler profiler = AsyncProfiler.getInstance();
    final Class<T> handlerClass;
    final Class<I> typeParameterClass;

    public ServiceImpl(
            Class<T> handlerClass,
            Class<I> typeParameterClass) {
        this.handlerClass = handlerClass;
        this.typeParameterClass = typeParameterClass;
    }

    public void execute(WorkerProto.WorkerRequest request,
                        StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        I input = JSON.parseObject(request.getJson(), typeParameterClass);
        O output;

        boolean isProfile = Boolean.parseBoolean(System.getenv("PROFILING_ENABLED"));
        try {
            String requestId = String.format("%d_%s", input.getQueryId(), UUID.randomUUID());
            WorkerContext context = new WorkerContext(LogManager.getLogger(handlerClass), new WorkerMetrics(), requestId);
            RequestHandler<I, O> handler = handlerClass.getConstructor(WorkerContext.class).newInstance(context);

            if (isProfile) {
                log.info(String.format("enable profile to execute input: %s", JSON.toJSONString(input)));
                String fileName = String.format("%s.jfr", handler.getRequestId());
                String event = System.getenv("PROFILING_EVENT");

                profiler.execute(String.format("start,jfr,threads,event=%s,file=%s", event, fileName));
                output = handler.handleRequest(input);
                profiler.execute(String.format("stop,file=%s", fileName));

                // store the JFR profiling file to FTP server
                String hostname = System.getenv("FTP_HOST");
                int port = Integer.parseInt(System.getenv("FTP_PORT"));
                FTPClient ftpClient = new FTPClient();
                ftpClient.connect(hostname, port);
                String username = System.getenv("FTP_USERNAME");
                String password = System.getenv("FTP_PASSWORD");
                ftpClient.login(username, password);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
//                log.info("connect to FTP server successfully");
                FileInputStream inputStream = new FileInputStream(fileName);
                ftpClient.storeFile("experiments/" + fileName, inputStream);
                log.info("store profiling file to FTP server successfully");
                inputStream.close();
                ftpClient.logout();
            } else {
                log.info(String.format("disable profile to execute input: %s", JSON.toJSONString(input)));
                output = handler.handleRequest(input);
            }
            log.info(String.format("get output successfully: %s", JSON.toJSONString(output)));
        } catch (Exception e) {
            throw new RuntimeException("Exception during process: ", e);
        }
        WorkerProto.WorkerResponse response = WorkerProto.WorkerResponse.newBuilder()
                .setJson(JSON.toJSONString(output))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
