package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import io.pixelsdb.pixels.worker.common.WorkerServiceGrpc;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerSyncClient {
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub stub;

    public WorkerSyncClient(String host, int port) {
        checkArgument(host != null, "illegal rpc host");
        ;
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = WorkerServiceGrpc.newBlockingStub(channel);
    }


    public void shutdown() throws InterruptedException {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public ConnectivityState getState() throws InterruptedException {
        return this.channel.getState(true);
    }

    public String hello(String username) {
        WorkerProto.HelloRequest request = WorkerProto.HelloRequest.newBuilder()
                .setName(username)
                .build();

        WorkerProto.HelloResponse response = this.stub.hello(request);
        return response.getOutput();
    }

    public AggregationOutput aggregation(AggregationInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        WorkerProto.WorkerResponse response = this.stub.aggregation(request);
        AggregationOutput output = JSON.parseObject(response.getJson(), AggregationOutput.class);
        return output;
    }

    public JoinOutput broadcastChainJoin(BroadcastChainJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        WorkerProto.WorkerResponse response = this.stub.broadcastChainJoin(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput broadcastJoin(BroadcastJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        WorkerProto.WorkerResponse response = this.stub.broadcastJoin(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput partitionChainJoin(PartitionedChainJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        WorkerProto.WorkerResponse response = this.stub.partitionChainJoin(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput partitionJoin(PartitionedJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        WorkerProto.WorkerResponse response = this.stub.partitionJoin(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public PartitionOutput partition(PartitionInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        WorkerProto.WorkerResponse response = this.stub.partition(request);
        PartitionOutput output = JSON.parseObject(response.getJson(), PartitionOutput.class);
        return output;
    }

    public ScanOutput scan(ScanInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();

        WorkerProto.WorkerResponse response = this.stub.scan(request);
        ScanOutput output = JSON.parseObject(response.getJson(), ScanOutput.class);
        return output;
    }
}
