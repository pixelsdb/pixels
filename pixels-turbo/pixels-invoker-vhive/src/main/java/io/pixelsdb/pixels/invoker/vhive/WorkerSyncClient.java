/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.turbo.vHiveWorkerServiceGrpc;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerSyncClient
{
    private final ManagedChannel channel;
    private final vHiveWorkerServiceGrpc.vHiveWorkerServiceBlockingStub stub;

    public WorkerSyncClient(String host, int port)
    {
        checkArgument(host != null, "illegal rpc host");
        ;
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = vHiveWorkerServiceGrpc.newBlockingStub(channel);
    }


    public void shutdown() throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public ConnectivityState getState() throws InterruptedException
    {
        return this.channel.getState(true);
    }

    public AggregationOutput aggregation(AggregationInput input)
    {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.AGGREGATION))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        TurboProto.WorkerResponse response = this.stub.process(request);
        AggregationOutput output = JSON.parseObject(response.getJson(), AggregationOutput.class);
        return output;
    }

    public JoinOutput broadcastChainJoin(BroadcastChainJoinInput input)
    {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.BROADCAST_CHAIN_JOIN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        TurboProto.WorkerResponse response = this.stub.process(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput broadcastJoin(BroadcastJoinInput input)
    {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.BROADCAST_JOIN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        TurboProto.WorkerResponse response = this.stub.process(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput partitionChainJoin(PartitionedChainJoinInput input)
    {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.PARTITIONED_CHAIN_JOIN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        TurboProto.WorkerResponse response = this.stub.process(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput partitionJoin(PartitionedJoinInput input)
    {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.PARTITIONED_JOIN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        TurboProto.WorkerResponse response = this.stub.process(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public PartitionOutput partition(PartitionInput input)
    {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.PARTITION))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();
        TurboProto.WorkerResponse response = this.stub.process(request);
        PartitionOutput output = JSON.parseObject(response.getJson(), PartitionOutput.class);
        return output;
    }

    public ScanOutput scan(ScanInput input)
    {
        TurboProto.WorkerRequest request = TurboProto.WorkerRequest.newBuilder()
                .setWorkerType(String.valueOf(WorkerType.SCAN))
                .setJson(JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect))
                .build();

        TurboProto.WorkerResponse response = this.stub.process(request);
        ScanOutput output = JSON.parseObject(response.getJson(), ScanOutput.class);
        return output;
    }
}
