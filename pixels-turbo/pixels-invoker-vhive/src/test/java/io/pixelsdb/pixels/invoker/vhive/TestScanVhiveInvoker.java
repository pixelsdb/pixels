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
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class TestScanVhiveInvoker
{
    @Test
    public void testScan() throws ExecutionException, InterruptedException
    {
        StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio,
                ConfigFactory.Instance().getProperty("minio.region"),
                ConfigFactory.Instance().getProperty("minio.endpoint"),
                ConfigFactory.Instance().getProperty("minio.access.key"),
                ConfigFactory.Instance().getProperty("minio.secret.key"));

        ScanOutput output = (ScanOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.SCAN).invoke(Utils.genScanInput(storageInfo, 0)).get();
        System.out.println(JSON.toJSONString(output));
    }
}
