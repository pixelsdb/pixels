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
package io.pixelsdb.pixels.invoker.lambda;

import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import org.junit.Test;

/**
 * @author hank
 * @create 2022-08-12
 */
public class TestLambdaInvoker
{
    @Test
    public void test()
    {
        int memorySize = InvokerFactory.Instance().getInvoker(WorkerType.PARTITIONED_JOIN).getMemoryMB();
        System.out.println(memorySize);
    }
}
