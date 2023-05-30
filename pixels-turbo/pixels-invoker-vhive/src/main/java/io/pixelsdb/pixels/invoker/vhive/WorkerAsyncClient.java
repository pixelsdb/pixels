package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.turbo.vHiveWorkerServiceGrpc;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerAsyncClient {
    private final ManagedChannel channel;
    private final vHiveWorkerServiceGrpc.vHiveWorkerServiceFutureStub stub;

    public WorkerAsyncClient(String host, int port) {
        checkArgument(host != null, "illegal rpc host");
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = vHiveWorkerServiceGrpc.newFutureStub(channel);
    }

    public void shutdown() throws InterruptedException {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public ConnectivityState getState() throws InterruptedException {
        return this.channel.getState(true);
    }

    public ListenableFuture<TurboProto.GetMemoryResponse> getMemory() {
        TurboProto.GetMemoryRequest request = TurboProto.GetMemoryRequest.newBuilder()
                .build();
        return this.stub.getMemory(request);
    }

    public ListenableFuture<TurboProto.WorkerResponse> aggregation(AggregationInput input) {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.AGGREGATION))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        return this.stub.process(request);
    }

    public ListenableFuture<TurboProto.WorkerResponse> broadcastChainJoin(BroadcastChainJoinInput input) {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.BROADCAST_CHAIN_JOIN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        return this.stub.process(request);
    }

    public ListenableFuture<TurboProto.WorkerResponse> broadcastJoin(BroadcastJoinInput input) {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.BROADCAST_JOIN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        return this.stub.process(request);
    }

    public ListenableFuture<TurboProto.WorkerResponse> partitionChainJoin(PartitionedChainJoinInput input) {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.PARTITIONED_CHAIN_JOIN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        return this.stub.process(request);
    }

    public ListenableFuture<TurboProto.WorkerResponse> partitionJoin(PartitionedJoinInput input) {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.PARTITIONED_JOIN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        return this.stub.process(request);
    }

    public ListenableFuture<TurboProto.WorkerResponse> partition(PartitionInput input) {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.PARTITION))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        return this.stub.process(request);
    }

    public ListenableFuture<TurboProto.WorkerResponse> scan(ScanInput input) {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.SCAN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        return this.stub.process(request);
    }
}
