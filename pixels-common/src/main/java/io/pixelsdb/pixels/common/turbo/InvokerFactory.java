/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.common.turbo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hank
 * @date 6/28/22
 */
public class InvokerFactory
{
    private static final InvokerFactory instance = new InvokerFactory();

    public static InvokerFactory Instance()
    {
        return instance;
    }

    /**
     * registerInvoker and getInvoker are not likely to be called concurrently.
     * But we still use concurrent hash map to avoid any possible concurrent retrievals and updates.
     */
    private final Map<WorkerType, Invoker> invokerMap = new ConcurrentHashMap<>();

    private InvokerFactory() { }

    /**
     * Register the invoker implementations for the corresponding cloud function workers.
     * It should be called before getInvoker.
     * @param producer
     */
    public void registerInvokers(InvokerProducer producer)
    {
        for (WorkerType workerType : WorkerType.values())
        {
            Invoker invoker = producer.produce(workerType);
            if (invoker != null)
            {
                this.invokerMap.put(workerType, invoker);
            }
        }
    }

    public Invoker getInvoker(WorkerType workerType)
    {
        return this.invokerMap.get(workerType);
    }
}
