package io.pixelsdb.pixels.worker.vhive;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.slf4j.LoggerFactory;

public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
    private final AggregationWorker aggregationWorker;
    private final BroadcastChainJoinWorker broadcastChainJoinWorker;
    private final BroadcastJoinWorker broadcastJoinWorker;
    private final PartitionedChainJoinWorker partitionedChainJoinWorker;
    private final PartitionedJoinWorker partitionedJoinWorker;
    private final PartitionWorker partitionWorker;
    private final ScanWorker scanWorker;

    public WorkerServiceImpl() {
        this.aggregationWorker = new AggregationWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(AggregationWorker.class),
                        new WorkerMetrics(),
                        "id_AggregationWorker"
                )
        );
        this.broadcastChainJoinWorker = new BroadcastChainJoinWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(BroadcastChainJoinWorker.class),
                        new WorkerMetrics(),
                        "id_BroadcastChainJoinWorker"
                )
        );
        this.broadcastJoinWorker = new BroadcastJoinWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(BroadcastJoinWorker.class),
                        new WorkerMetrics(),
                        "id_BroadcastJoinWorker"
                )
        );
        this.partitionedChainJoinWorker = new PartitionedChainJoinWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(PartitionedChainJoinWorker.class),
                        new WorkerMetrics(),
                        "id_PartitionedChainJoinWorker"
                )
        );
        this.partitionedJoinWorker = new PartitionedJoinWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(PartitionedJoinWorker.class),
                        new WorkerMetrics(),
                        "id_PartitionedJoinWorker"
                )
        );
        this.partitionWorker = new PartitionWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(PartitionWorker.class),
                        new WorkerMetrics(),
                        "id_PartitionWorker"
                )
        );
        this.scanWorker = new ScanWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(ScanWorker.class),
                        new WorkerMetrics(),
                        "id_ScanWorker"
                )
        );
    }

    @Override
    public void aggregation(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<AggregationInput, AggregationOutput> service = new ServiceImpl<>(this.aggregationWorker, AggregationInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void broadcastChainJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<BroadcastChainJoinInput, JoinOutput> service = new ServiceImpl<>(this.broadcastChainJoinWorker, BroadcastChainJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void broadcastJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<BroadcastJoinInput, JoinOutput> service = new ServiceImpl<>(this.broadcastJoinWorker, BroadcastJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partitionChainJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<PartitionedChainJoinInput, JoinOutput> service = new ServiceImpl<>(this.partitionedChainJoinWorker, PartitionedChainJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partitionJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<PartitionedJoinInput, JoinOutput> service = new ServiceImpl<>(this.partitionedJoinWorker, PartitionedJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partition(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<PartitionInput, PartitionOutput> service = new ServiceImpl<>(this.partitionWorker, PartitionInput.class);
        service.execute(request, responseObserver);
    }


    @Override
    public void scan(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<ScanInput, ScanOutput> service = new ServiceImpl<>(this.scanWorker, ScanInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void hello(WorkerProto.HelloRequest request, StreamObserver<WorkerProto.HelloResponse> responseObserver) {
        String output = "Hello, " + request.getName();

        WorkerProto.HelloResponse response = WorkerProto.HelloResponse.newBuilder().setOutput(output).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


}
