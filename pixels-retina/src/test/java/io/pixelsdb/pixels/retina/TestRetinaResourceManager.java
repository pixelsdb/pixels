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
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestRetinaResourceManager
{
    private final RetinaResourceManager retinaResourceManager = RetinaResourceManager.Instance();

    private void printVisibility(long[] visibility)
    {
        for (long num : visibility)
        {
            System.out.print(String.format(
                    "%64s", Long.toBinaryString(num)).replace(' ', '0'));
        }
        System.out.println();
    }

    private boolean checkVisibility(long[] visibility, int rowId)
    {
        long targetLong = visibility[rowId / 64];
        return (targetLong & (1L << (rowId % 64))) != 0;
    }

    private RetinaResourceManager newIsolatedManager() throws Exception
    {
        Constructor<RetinaResourceManager> constructor = RetinaResourceManager.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }

    private void setGcExecutor(RetinaResourceManager manager,
                               ScheduledExecutorService executor) throws Exception
    {
        Field field = RetinaResourceManager.class.getDeclaredField("gcExecutor");
        field.setAccessible(true);
        field.set(manager, executor);
    }

    @Test
    public void testBackgroundGcIsNotStartedByConstructor() throws Exception
    {
        Constructor<RetinaResourceManager> constructor = RetinaResourceManager.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        RetinaResourceManager manager = constructor.newInstance();

        assertFalse("background GC must be started by lifecycle only",
                manager.isBackgroundGcStarted());
    }

    @Test
    public void testStartBackgroundGcIsExplicitAndIdempotent() throws Exception
    {
        String originalInterval = ConfigFactory.Instance().getProperty("retina.gc.interval");
        RetinaResourceManager manager = newIsolatedManager();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        setGcExecutor(manager, executor);
        try
        {
            ConfigFactory.Instance().addProperty("retina.gc.interval", "300");

            manager.startBackgroundGc();
            manager.startBackgroundGc();

            assertTrue("explicit lifecycle start must mark background GC as started",
                    manager.isBackgroundGcStarted());
            verify(executor).scheduleAtFixedRate(any(Runnable.class), eq(300L), eq(300L), eq(TimeUnit.SECONDS));
            verifyNoMoreInteractions(executor);
        }
        finally
        {
            ConfigFactory.Instance().addProperty("retina.gc.interval", originalInterval);
        }
    }

    @Test
    public void testStartBackgroundGcDisabledByNonPositiveInterval() throws Exception
    {
        String originalInterval = ConfigFactory.Instance().getProperty("retina.gc.interval");
        RetinaResourceManager manager = newIsolatedManager();
        try
        {
            ConfigFactory.Instance().addProperty("retina.gc.interval", "0");

            manager.startBackgroundGc();

            assertFalse("disabled interval must not mark background GC as started",
                    manager.isBackgroundGcStarted());
        }
        finally
        {
            ConfigFactory.Instance().addProperty("retina.gc.interval", originalInterval);
        }
    }

    @Test
    public void testStartBackgroundGcInvalidIntervalFailsWithoutStarting() throws Exception
    {
        String originalInterval = ConfigFactory.Instance().getProperty("retina.gc.interval");
        RetinaResourceManager manager = newIsolatedManager();
        try
        {
            ConfigFactory.Instance().addProperty("retina.gc.interval", "not-a-number");

            try
            {
                manager.startBackgroundGc();
                fail("invalid GC interval must fail closed");
            }
            catch (RetinaException e)
            {
                assertTrue(e.getMessage().contains("Invalid retina GC interval configuration"));
            }

            assertFalse("failed lifecycle start must not mark GC as started",
                    manager.isBackgroundGcStarted());
        }
        finally
        {
            ConfigFactory.Instance().addProperty("retina.gc.interval", originalInterval);
        }
    }

    @Test
    public void testStartBackgroundGcSchedulerFailureRollsBackStartedFlag() throws Exception
    {
        String originalInterval = ConfigFactory.Instance().getProperty("retina.gc.interval");
        RetinaResourceManager manager = newIsolatedManager();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        setGcExecutor(manager, executor);
        try
        {
            ConfigFactory.Instance().addProperty("retina.gc.interval", "300");
            when(executor.scheduleAtFixedRate(any(Runnable.class), eq(300L), eq(300L), eq(TimeUnit.SECONDS)))
                    .thenThrow(new RuntimeException("scheduler rejected"));

            try
            {
                manager.startBackgroundGc();
                fail("scheduler failure must fail closed");
            }
            catch (RetinaException e)
            {
                assertTrue(e.getMessage().contains("Failed to start retina background GC"));
            }

            assertFalse("scheduler failure must roll back started flag",
                    manager.isBackgroundGcStarted());
        }
        finally
        {
            ConfigFactory.Instance().addProperty("retina.gc.interval", originalInterval);
        }
    }

    @Test
    public void testRunGcBeforeLifecycleStartIsRejected() throws Exception
    {
        RetinaResourceManager manager = newIsolatedManager();
        Method runGc = RetinaResourceManager.class.getDeclaredMethod("runGC");
        runGc.setAccessible(true);

        runGc.invoke(manager);

        assertFalse("manual GC invocation before lifecycle start must be ignored",
                manager.isBackgroundGcStarted());
    }

    @Test
    public void TestVisibility()
    {
        try
        {
            long fileId = 999;
            int rgId = 666;
            int recordNum = 100;
            retinaResourceManager.addVisibility(fileId, rgId, recordNum, 0L, null, false);
            long [] visibility = retinaResourceManager.queryVisibility(fileId, rgId, 0);
            printVisibility(visibility);

            // delete row
            retinaResourceManager.deleteRecord(fileId, rgId, 33, 5);
            visibility = retinaResourceManager.queryVisibility(fileId, rgId, 0);
            printVisibility(visibility);
            visibility = retinaResourceManager.queryVisibility(fileId, rgId, 10);
            printVisibility(visibility);
            System.out.println(checkVisibility(visibility, 33));
        } catch (RetinaException e)
        {
            throw new RuntimeException(e);
        }
    }

    private byte[][] createTpchNationRow(long nationKey, String name, long regionKey, String comment)
    {
        byte[][] row  = new byte[4][];
        row[0] = ByteBuffer.allocate(8).putLong(nationKey).array();  // bigint
        row[1] = name.getBytes();                                      // char(25)
        row[2] = ByteBuffer.allocate(8).putLong(regionKey).array();  // bigint
        row[3] = comment.getBytes();                                   // varchar(152)
        return row;
    }

    @Ignore("Integration test requires real tpch.nation metadata and storage state.")
    @Test
    public void testWriteBuffer()
    {
        try
        {
            String schemaName = "tpch";
            String tableName = "nation";
            retinaResourceManager.addWriteBuffer(schemaName, tableName);

            // insert data
            byte[][] colValues = createTpchNationRow(2333, "ALGERIA",
                    0, "haggle. carefully final deposits detect slyly agai");
            retinaResourceManager.insertRecord(schemaName, tableName, colValues, 5, 0);
            RetinaProto.GetWriteBufferResponse response =
                    retinaResourceManager.getWriteBuffer(schemaName, tableName, 0, 0).build();
            System.out.println(response.getData().isEmpty() ? "empty"  :
                    VectorizedRowBatch.deserialize(response.getData().toByteArray()));
            System.out.println(response.getBitmaps(0).getBitmapCount());
            System.out.println(response.getBitmaps(0));

            // delete record
            long fileId = 323;  // obtained during debug
            int rgId = 0;
            int rgRowId = 0;
            retinaResourceManager.deleteRecord(fileId, rgId, rgRowId, 10);
            long [] visibility = retinaResourceManager.queryVisibility(fileId, rgId, 0);
            printVisibility(visibility);
            visibility = retinaResourceManager.queryVisibility(fileId, rgId, 10);
            printVisibility(visibility);
            System.out.println(checkVisibility(visibility, rgRowId));
        } catch (RetinaException e)
        {
            throw new RuntimeException(e);
        }
    }
}
