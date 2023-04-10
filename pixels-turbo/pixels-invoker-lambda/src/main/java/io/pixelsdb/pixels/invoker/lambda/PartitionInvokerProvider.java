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
package io.pixelsdb.pixels.invoker.lambda;

import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static io.pixelsdb.pixels.common.turbo.FunctionService.lambda;
import static io.pixelsdb.pixels.common.turbo.WorkerType.PARTITION;

/**
 * Created at: 2023-04-10
 * Author: hank
 */
public class PartitionInvokerProvider implements InvokerProvider
{
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker()
    {
        String partitionWorker = config.getProperty("partition.worker.name");
        return new PartitionInvoker(partitionWorker);
    }

    @Override
    public WorkerType workerType()
    {
        return PARTITION;
    }

    @Override
    public boolean belongsTo(FunctionService functionService)
    {
        return functionService.equals(lambda);
    }
}
