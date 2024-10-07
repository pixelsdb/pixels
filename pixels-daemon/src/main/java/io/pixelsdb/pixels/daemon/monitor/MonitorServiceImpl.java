package io.pixelsdb.pixels.daemon.monitor;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.daemon.MonitorProto;
import io.pixelsdb.pixels.daemon.MonitorServiceGrpc;
import io.pixelsdb.pixels.daemon.monitor.util.PolicyManager;

import java.util.concurrent.BlockingDeque;

public class MonitorServiceImpl extends MonitorServiceGrpc.MonitorServiceImplBase {
    private final BlockingDeque<Integer> metricsQueue;

    public MonitorServiceImpl() {
        metricsQueue = MetricsQueue.queue;
        new Thread(new PolicyManager()).start();
    }

    @Override
    public StreamObserver<MonitorProto.MonitorRequest> collectMetrics(StreamObserver<MonitorProto.MonitorResponse> responseObserver) {
        return new StreamObserver<MonitorProto.MonitorRequest>() {
            @Override
            public void onNext(MonitorProto.MonitorRequest request) {
                metricsQueue.add(request.getQueryConcurrency());
                System.out.println("Now concurrency is " + metricsQueue);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Client send metrics error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("Client completed sending metrics");

            }
        };
    }

    @Override
    public void checkMetrics(MonitorProto.CheckRequest request, StreamObserver<MonitorProto.CheckResponse> responseObserver) {
        super.checkMetrics(request, responseObserver);
    }
}