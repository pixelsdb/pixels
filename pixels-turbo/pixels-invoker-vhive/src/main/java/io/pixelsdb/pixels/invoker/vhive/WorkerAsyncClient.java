package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import io.pixelsdb.pixels.worker.common.WorkerServiceGrpc;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerAsyncClient {
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceFutureStub stub;

    public WorkerAsyncClient(String host, int port) {
        checkArgument(host != null, "illegal rpc host");
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = WorkerServiceGrpc.newFutureStub(channel);
    }

    public void shutdown() throws InterruptedException {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public ConnectivityState getState() throws InterruptedException {
        return this.channel.getState(true);
    }

    public ListenableFuture<WorkerProto.HelloResponse> hello(String username) {
        WorkerProto.HelloRequest request = WorkerProto.HelloRequest.newBuilder()
                .setName(username)
                .build();

        return this.stub.hello(request);
    }

    public ListenableFuture<WorkerProto.GetMemoryResponse> getMemory() {
        WorkerProto.GetMemoryRequest request = WorkerProto.GetMemoryRequest.newBuilder()
                .build();
        return this.stub.getMemory(request);
    }

    public ListenableFuture<WorkerProto.WorkerResponse> aggregation(AggregationInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        return this.stub.aggregation(request);
    }

    public ListenableFuture<WorkerProto.WorkerResponse> broadcastChainJoin(BroadcastChainJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        return this.stub.broadcastChainJoin(request);
    }

    public ListenableFuture<WorkerProto.WorkerResponse> broadcastJoin(BroadcastJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        return this.stub.broadcastJoin(request);
    }

    public ListenableFuture<WorkerProto.WorkerResponse> partitionChainJoin(PartitionedChainJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        return this.stub.partitionChainJoin(request);
    }

    public ListenableFuture<WorkerProto.WorkerResponse> partitionJoin(PartitionedJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        return this.stub.partitionJoin(request);
    }

    public ListenableFuture<WorkerProto.WorkerResponse> partition(PartitionInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        return this.stub.partition(request);
    }

    public ListenableFuture<WorkerProto.WorkerResponse> scan(ScanInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();

        return this.stub.scan(request);
    }
}
