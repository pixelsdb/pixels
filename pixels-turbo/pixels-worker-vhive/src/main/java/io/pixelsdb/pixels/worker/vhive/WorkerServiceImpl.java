package io.pixelsdb.pixels.worker.vhive;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import io.pixelsdb.pixels.worker.common.WorkerServiceGrpc;
import io.pixelsdb.pixels.worker.vhive.utils.ServiceImpl;

public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
    public WorkerServiceImpl() {
    }

    @Override
    public void aggregation(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<AggregationWorker, AggregationInput, AggregationOutput> service = new ServiceImpl<>(AggregationWorker.class, AggregationInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void broadcastChainJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<BroadcastChainJoinWorker, BroadcastChainJoinInput, JoinOutput> service = new ServiceImpl<>(BroadcastChainJoinWorker.class, BroadcastChainJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void broadcastJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<BroadcastJoinWorker, BroadcastJoinInput, JoinOutput> service = new ServiceImpl<>(BroadcastJoinWorker.class, BroadcastJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partitionChainJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<PartitionedChainJoinWorker, PartitionedChainJoinInput, JoinOutput> service = new ServiceImpl<>(PartitionedChainJoinWorker.class, PartitionedChainJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partitionJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<PartitionedJoinWorker, PartitionedJoinInput, JoinOutput> service = new ServiceImpl<>(PartitionedJoinWorker.class, PartitionedJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partition(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<PartitionWorker, PartitionInput, PartitionOutput> service = new ServiceImpl<>(PartitionWorker.class, PartitionInput.class);
        service.execute(request, responseObserver);
    }


    @Override
    public void scan(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ServiceImpl<ScanWorker, ScanInput, ScanOutput> service = new ServiceImpl<>(ScanWorker.class, ScanInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void hello(WorkerProto.HelloRequest request, StreamObserver<WorkerProto.HelloResponse> responseObserver) {
        String output = "Hello, " + request.getName();

        WorkerProto.HelloResponse response = WorkerProto.HelloResponse.newBuilder().setOutput(output).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    @Override
    public void getMemory(WorkerProto.GetMemoryRequest request, StreamObserver<WorkerProto.GetMemoryResponse> responseObserver) {
        // return the MB(1024 * 1024) size
        int dataSize = 1024 * 1024;
        WorkerProto.GetMemoryResponse response = WorkerProto.GetMemoryResponse.newBuilder()
                .setMemoryMB(Runtime.getRuntime().totalMemory() / dataSize)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
