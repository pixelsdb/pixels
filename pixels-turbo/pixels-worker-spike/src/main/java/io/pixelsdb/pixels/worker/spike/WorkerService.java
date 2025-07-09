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
package io.pixelsdb.pixels.worker.spike;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.pixelsdb.pixels.common.physical.StorageProvider;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.spike.handler.SpikeWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ServiceLoader;

public class WorkerService<T extends WorkerInterface<I, O>, I extends Input, O extends Output>
{
    private static final Logger log = LogManager.getLogger(WorkerService.class);

    final Class<T> handlerClass;
    final Class<I> typeParameterClass;

    public WorkerService(Class<T> handlerClass, Class<I> typeParameterClass)
    {
        this.handlerClass = handlerClass;
        this.typeParameterClass = typeParameterClass;
    }

    public SpikeWorker.CallWorkerFunctionResp execute(String workerPayLoad, long requestId)
    {
        I input = JSON.parseObject(workerPayLoad, typeParameterClass);
        O output;
        try
        {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            ServiceLoader<StorageProvider> providerLoader = ServiceLoader.load(StorageProvider.class, contextClassLoader);
            int classCnt = 0;
            log.debug("Current ClassLoader: " + Thread.currentThread().getContextClassLoader());
            for (StorageProvider storageProvider : providerLoader)
            {
                log.debug(String.format("storageProvider class: %s", storageProvider.getClass().getName()));
                classCnt++;
            }
            log.debug(String.format("classCnt: %d", classCnt));
            WorkerContext context = new WorkerContext(LogManager.getLogger(handlerClass), new WorkerMetrics(), Long.toString(requestId));
            WorkerInterface<I, O> worker = handlerClass.getConstructor(WorkerContext.class).newInstance(context);
            log.info(String.format("execute input: %s",
                    JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect)));
            output = worker.handleRequest(input);
            log.info(String.format("get output successfully: %s", JSON.toJSONString(output)));
        } catch (Exception e)
        {
            throw new RuntimeException("Exception during process: ", e);
        }
        return SpikeWorker.CallWorkerFunctionResp.newBuilder()
                .setRequestId(requestId)
                .setPayload(JSON.toJSONString(output))
                .build();
    }
}
