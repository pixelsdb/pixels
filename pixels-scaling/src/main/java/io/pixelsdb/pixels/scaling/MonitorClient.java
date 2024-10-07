package io.pixelsdb.pixels.scaling;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.MonitorProto;
import io.pixelsdb.pixels.daemon.MonitorServiceGrpc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class MonitorClient {
    private final ManagedChannel managerChannel;
    private final MonitorServiceGrpc.MonitorServiceStub monitorServiceStub;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private String monitorName;


    public MonitorClient(int port) {
        String host = ConfigFactory.Instance().getProperty("monitor.server.host");
        managerChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        monitorServiceStub = MonitorServiceGrpc.newStub(managerChannel);
        monitorName = "default";
    }

    public MonitorClient(String name, int port) {
        this(port);
        monitorName = name;
    }

    public void reportMetric(int concurrency) {
        StreamObserver<MonitorProto.MonitorResponse> responseStreamObserver = new StreamObserver<MonitorProto.MonitorResponse>() {
            @Override
            public void onNext(MonitorProto.MonitorResponse monitorResponse) {
                System.out.println("Received response");
            }

            @Override
            public void onError(Throwable throwable) {
                System.err.println("Error: " + throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("Stream completed");

            }
        };
        StreamObserver<MonitorProto.MonitorRequest> requestObserver = monitorServiceStub.collectMetrics(responseStreamObserver);
        MonitorProto.MonitorRequest request = MonitorProto.MonitorRequest.newBuilder()
                .setQueryConcurrency(concurrency)
                .build();
        requestObserver.onNext(request);
        System.out.println("Send metric: " + request.getQueryConcurrency());
    }

    public void stopReportMetric() {
        if (managerChannel != null) {
            managerChannel.shutdown();
        }
    }
}
