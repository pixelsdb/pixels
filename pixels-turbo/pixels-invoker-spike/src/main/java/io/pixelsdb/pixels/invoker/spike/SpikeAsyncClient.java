/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.invoker.spike;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class SpikeAsyncClient
{
    private static final ConfigFactory config = ConfigFactory.Instance();
    private static SpikeAsyncClient instance;
    private final ManagedChannel channel;
    private final SpikeServiceGrpc.SpikeServiceFutureStub stub;
    private final int cpu;
    private final int memory;
    private final int timeout;

    private SpikeAsyncClient()
    {
        this.cpu = Integer.parseInt(config.getProperty("spike.cpu"));
        this.memory = Integer.parseInt(config.getProperty("spike.memory"));
        this.timeout = Integer.parseInt(config.getProperty("spike.timeout"));
        String host = config.getProperty("spike.hostname");
        int port = Integer.parseInt(config.getProperty("spike.port"));
        checkArgument(host != null, "illegal rpc host");
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = SpikeServiceGrpc.newFutureStub(channel);
    }

    public static synchronized SpikeAsyncClient getInstance()
    {
        if (instance == null)
        {
            instance = new SpikeAsyncClient();
        }
        return instance;
    }

    @Override
    protected void finalize() throws Throwable
    {
        try
        {
            if (channel != null && !channel.isShutdown())
            {
                channel.shutdown();
            }
        } finally
        {
            super.finalize();
        }
    }

    public final ListenableFuture<SpikeServiceProto.CallFunctionResponse> invoke(String functionName, String payload, int requiredCpu, int requiredMemory)
    {
        SpikeServiceProto.CallFunctionRequest request = SpikeServiceProto.CallFunctionRequest.newBuilder()
                .setFunctionName(functionName)
                .setCpu(requiredCpu)
                .setMemory(requiredMemory)
                .setPayload(payload)
                .build();
        return this.stub.withDeadlineAfter(this.timeout, TimeUnit.SECONDS).callFunction(request);
    }

    public final ListenableFuture<SpikeServiceProto.CallFunctionResponse> invoke(String functionName, String payload)
    {
        SpikeServiceProto.CallFunctionRequest request = SpikeServiceProto.CallFunctionRequest.newBuilder()
                .setFunctionName(functionName)
                .setCpu(this.cpu)
                .setMemory(this.memory)
                .setPayload(payload)
                .build();
        return this.stub.withDeadlineAfter(this.timeout, TimeUnit.SECONDS).callFunction(request);
    }
}