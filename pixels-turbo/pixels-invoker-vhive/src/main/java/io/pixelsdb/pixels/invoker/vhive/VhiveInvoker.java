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

import com.google.common.util.concurrent.ListenableFuture;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.invoker.vhive.utils.ListenableFutureAdapter;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public abstract class VhiveInvoker implements Invoker
{
    private static final Logger logger = LogManager.getLogger(VhiveInvoker.class);
    private final String functionName;
    private int memoryMB;

    protected VhiveInvoker(String functionName)
    {
        this.functionName = functionName;
        new Thread(() -> {
            int memoryMB = 0;
            try
            {
                TurboProto.GetMemoryResponse response = Vhive.Instance().getAsyncClient().getMemory().get();
                memoryMB = (int) response.getMemoryMB();
            } catch (Exception e)
            {
                logger.error("failed to get memory: " + e);
            }
            this.memoryMB = memoryMB;
        }).start();
    }

    @Override
    public String getFunctionName()
    {
        return functionName;
    }

    @Override
    public int getMemoryMB()
    {
        return memoryMB;
    }

    public CompletableFuture<Output> genCompletableFuture(ListenableFuture<TurboProto.WorkerResponse> listenableFuture)
    {
        CompletableFuture<TurboProto.WorkerResponse> completableFuture = ListenableFutureAdapter.toCompletable(listenableFuture);
        return completableFuture.handle((response, err) -> {
            if (err == null)
            {
                String outputJson = response.getJson();
                Output output = this.parseOutput(outputJson);
                if (output != null)
                {
                    output.setMemoryMB(this.memoryMB);
                    return output;
                } else
                {
                    throw new RuntimeException("failed to parse response payload, JSON = " +
                            response.getJson());
                }
            } else
            {
                throw new RuntimeException("failed to execute the request, function: " +
                        this.getFunctionName(), err);
            }
        });
    }
}
