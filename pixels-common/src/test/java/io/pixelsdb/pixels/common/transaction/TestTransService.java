/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.transaction;

import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.lease.Lease;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @create 2025-12-06
 */
public class TestTransService
{
    @Test
    public void testExtendTransLease() throws TransException, InterruptedException
    {
        TransService service = TransService.Instance();
        TransContext context = service.beginTrans(false);
        Thread.sleep(5000);
        service.extendTransLease(context);
        service.commitTrans(context.getTransId(), false);
    }

    @Test
    public void testExtendTransLeaseBatch() throws TransException, InterruptedException
    {
        TransService service = TransService.Instance();
        List<TransContext> contexts = service.beginTransBatch(10, false);
        Thread.sleep(35000);
        List<Long> transIds = new ArrayList<>();
        for (TransContext context : contexts)
        {
            transIds.add(context.getTransId());
        }
        List<Boolean> res = service.extendTransLeaseBatch(contexts);
        for (int i = 0; i < res.size(); i++)
        {
            Assert.assertTrue(!res.get(i));
            Assert.assertTrue(contexts.get(i).getLease().expiring(System.currentTimeMillis(), Lease.Role.Holder));
            Assert.assertTrue(contexts.get(i).getLease().hasExpired(System.currentTimeMillis(), Lease.Role.Holder));
        }
        service.commitTransBatch(transIds, false);
    }

    @Test
    public void testPushWatermarks1() throws TransException, InterruptedException
    {
        TransService service = TransService.Instance();
        TransContext ctxT1 = service.beginTrans(false);
        Thread.sleep(1000);
        TransContext ctxT2 = service.beginTrans(false);
        Thread.sleep(1000);
        TransContext ctxT3 = service.beginTrans(false);
        Thread.sleep(1000);
        service.commitTrans(ctxT2.getTransId(), false);
        Thread.sleep(1000);
        service.commitTrans(ctxT3.getTransId(), false);
        Thread.sleep(1000);
        service.commitTrans(ctxT1.getTransId(), false);
        TransContext ctxQ1 = service.beginTrans(true);
        Assert.assertEquals(ctxQ1.getTimestamp(), ctxT3.getTimestamp());
        service.commitTrans(ctxQ1.getTransId(), true);
    }

    @Test
    public void testPushWatermarks2() throws TransException, InterruptedException
    {
        TransService service = TransService.Instance();
        TransContext ctxT1 = service.beginTrans(false);
        Thread.sleep(1000);
        TransContext ctxT2 = service.beginTrans(false);
        Thread.sleep(1000);
        TransContext ctxT3 = service.beginTrans(false);
        Thread.sleep(1000);
        TransContext ctxT4 = service.beginTrans(false);
        Thread.sleep(1000);
        service.commitTrans(ctxT2.getTransId(), false);
        Thread.sleep(1000);
        service.commitTrans(ctxT3.getTransId(), false);
        Thread.sleep(1000);
        service.commitTrans(ctxT1.getTransId(), false);
        TransContext ctxQ1 = service.beginTrans(true);
        Assert.assertEquals(ctxQ1.getTimestamp(), ctxT3.getTimestamp());
        service.commitTrans(ctxQ1.getTransId(), true);
        service.commitTrans(ctxT4.getTransId(), false);
    }
}
