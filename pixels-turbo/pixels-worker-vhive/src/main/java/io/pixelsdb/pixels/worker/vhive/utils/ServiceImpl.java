package io.pixelsdb.pixels.worker.vhive.utils;

import com.alibaba.fastjson.JSON;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import one.profiler.AsyncProfiler;
import org.apache.commons.net.ftp.FTPClient;

import java.io.FileInputStream;

public class ServiceImpl<I extends Input, O extends Output> {
    private static final ConfigFactory config = ConfigFactory.Instance();
    private static final AsyncProfiler profiler = AsyncProfiler.getInstance();
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
        O output;

        boolean isProfile = Boolean.parseBoolean(config.getProperty("vhive.profiling.enabled"));
        try {
            if (isProfile) {
                String fileName = "myprofile.jfr";
                profiler.execute(String.format("start,jfr,threads,event=wall,file=%s", fileName));
                output = handler.handleRequest(input);
//            Thread.sleep(60 * 1000);
                profiler.execute(String.format("stop,file=%s", fileName));

                // store the JFR profiling file to FTP server
                String hostname = config.getProperty("vhive.profiling.ftp.hostname");
                int port = Integer.parseInt(config.getProperty("vhive.profiling.ftp.port"));
                FTPClient ftpClient = new FTPClient();
                ftpClient.connect(hostname, port);
                String username = config.getProperty("vhive.profiling.ftp.username");
                String password = config.getProperty("vhive.profiling.ftp.password");
                ftpClient.login(username, password);
                FileInputStream inputStream = new FileInputStream(fileName);
                ftpClient.storeFile("experiments/" + fileName, inputStream);
                ftpClient.logout();
            } else {
                output = handler.handleRequest(input);
            }
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
