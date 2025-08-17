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

import java.util.concurrent.CompletableFuture;

/**
 * The interface for the invokers of cloud function (serverless) worker.
 * @create 2023-04-06
 * @author hank
 */
public interface Invoker
{
    /**
     * Parse a string output in Json format to the output object.
     * @param outputJson the Json output
     * @return the output object
     */
    Output parseOutput(String outputJson);

    /**
     * Invoke the cloud function. This creates a new worker of the cloud function on success.
     * @param input the input of the cloud function
     * @return the output of the cloud function
     */
    CompletableFuture<Output> invoke(Input input);

    /**
     * @return name of the invoked cloud function.
     */
    String getFunctionName();

    /**
     * @return the template memory size in MB of the invoked cloud function.
     */
    int getMemoryMB();
}
