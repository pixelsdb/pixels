package io.pixelsdb.pixels.daemon.monitor;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.daemon.MonitorProto;
import io.pixelsdb.pixels.daemon.MonitorServiceGrpc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class MockClient {
    private final ManagedChannel managerChannel;
    private final MonitorServiceGrpc.MonitorServiceStub monitorServiceStub;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public MockClient(int port) {
        managerChannel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        monitorServiceStub = MonitorServiceGrpc.newStub(managerChannel);
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
