package io.pixelsdb.pixels.worker.vhive.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

            String JSONFilename = String.format("%s.json", handler.getRequestId());
            if (isProfile) {
                log.info(String.format("enable profile to execute input: %s", JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect)));
                String JFRFilename = String.format("%s.jfr", handler.getRequestId());
                String event = System.getenv("PROFILING_EVENT");

                profiler.execute(String.format("start,jfr,threads,event=%s,file=%s", event, JFRFilename));
                output = handler.handleRequest(input);
                profiler.execute(String.format("stop,file=%s", JFRFilename));

                upload(JFRFilename, "experiments/" + JFRFilename);
            } else {
                log.info(String.format("disable profile to execute input: %s", JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect)));
                output = handler.handleRequest(input);
            }
            dump(JSONFilename, JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect));
            upload(JSONFilename, "experiments/" + JSONFilename);

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

    private void upload(String src, String dest) throws IOException {
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

        FileInputStream inputStream = new FileInputStream(src);
        ftpClient.storeFile(dest, inputStream);
        log.info(String.format("store file %s to FTP server %s successfully", src, dest));
        inputStream.close();
        ftpClient.logout();
    }

    private void dump(String filename, String content) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(filename);
        outputStream.write(content.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
    }
}
