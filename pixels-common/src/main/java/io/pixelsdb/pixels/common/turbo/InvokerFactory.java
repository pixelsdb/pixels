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

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hank
 * @create 2022-06-28
 */
public class InvokerFactory
{
    private static final class InstanceHolder
    {
        private static final InvokerFactory instance = new InvokerFactory();
    }

    public static InvokerFactory Instance()
    {
        return InstanceHolder.instance;
    }

    /**
     * registerInvoker and getInvoker are not likely to be called concurrently.
     * But we still use concurrent hash map to avoid any possible concurrent retrievals and updates.
     */
    private final Map<WorkerType, Invoker> invokerMap = new ConcurrentHashMap<>();
    private final ServiceLoader<InvokerProvider> providerLoader = ServiceLoader.load(InvokerProvider.class);
    private final FunctionService functionServiceInUse;

    private InvokerFactory()
    {
        String serviceName = ConfigFactory.Instance().getProperty("executor.function.service");
        functionServiceInUse = FunctionService.from(serviceName);
        this.providerLoader.forEach(invokerProvider -> {
            if (invokerProvider.compatibleWith(functionServiceInUse))
            {
                this.invokerMap.put(invokerProvider.workerType(), invokerProvider.createInvoker());
            }
        });
    }

    /**
     * Reload the invokers of the serverless workers. NOTE that this method is not thread-safe.
     * Other methods should not be called before this method returns.
     */
    public void reload()
    {
        this.providerLoader.reload();
        this.invokerMap.clear();
        this.providerLoader.forEach(invokerProvider -> {
            if (invokerProvider.compatibleWith(functionServiceInUse))
            {
                this.invokerMap.put(invokerProvider.workerType(), invokerProvider.createInvoker());
            }
        });
    }

    /**
     * Get the cloud function invoker of a worker type.
     * @param workerType the worker type
     * @return the cloud function invoker
     */
    public Invoker getInvoker(WorkerType workerType)
    {
        return this.invokerMap.get(workerType);
    }
}
