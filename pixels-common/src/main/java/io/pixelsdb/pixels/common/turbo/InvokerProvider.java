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
package io.pixelsdb.pixels.common.turbo;

/**
 * The SPI for the invoker provider of serverless workers.
 * In pixels.properties, set executor.function.service to the cloud function service that the invoker
 * implementations are compatible with.
 * In Pixels, there should be at most one set of invoker implementations compatible with each cloud
 * function service. Each implementation corresponds to one type of serverless worker.
 * <br/>
 * Created at: 4/6/23
 * Author: hank
 */
public interface InvokerProvider
{
    /**
     * Create an invoker instance of a serverless worker.
     */
    Invoker createInvoker();

    /**
     * @return the worker type of the invoker created by this provider.
     */
    WorkerType workerType();

    /**
     * @param functionService the given cloud function service.
     * @return true if this invoker provider is compatible with the given cloud function service.
     */
    boolean compatibleWith(FunctionService functionService);
}
