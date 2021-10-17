/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common;

import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.io.S3OutputStream;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created at: 9/8/21
 * Author: hank
 */
public class TestS3
{
    @Test
    public void testStorageScheme()
    {
        System.out.println(Storage.Scheme.fromPath("s3://container/object"));
    }

    @Test
    public void testS3Writer() throws IOException
    {
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(Storage.Scheme.s3, "pixels-01/object-4",
                0, (short) 1, false);
        ByteBuffer buffer = ByteBuffer.allocate(10240);
        buffer.putLong(1);
        writer.append(buffer);
        writer.flush();
        writer.close();
    }

    @Test
    public void testFuture()
    {
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        ByteBuffer[] buffer = {null};
        future.thenAccept(resp -> buffer[0] = resp);

        for (int i = 0; i < 1000_000_0; ++i)
        {
            ByteBuffer.allocate(1000_00);
        }
    }

    @Test
    public void testS3OutputStream() throws IOException
    {
        S3Client s3 = S3Client.builder().build();
        InputStream input = new FileInputStream("/home/hank/test.csv");
        OutputStream output = new S3OutputStream(s3, "pixels-01", "object-6");
        IOUtils.copyBytes(input, output, 1024*1024, true);
    }

    @Test
    public void testS3Download() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage("s3://pixels-01/test.pxl");
        InputStream input = storage.open("s3://pixels-01/test.pxl");
        OutputStream output = new FileOutputStream("test.pxl");
        IOUtils.copyBytes(input, output, 1024*1024, true);
    }

    @Test
    public void testGetStatus() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage("s3://pixels-01/test.pxl");
        Status status = storage.getStatus("s3://pixels-01/test.pxl");
        System.out.println(status.getLength());
        System.out.println(status.getName());
        System.out.println(status.getPath());
    }

    @Test
    public void testlistStatus() throws IOException, InterruptedException
    {
        Storage storage = StorageFactory.Instance().getStorage("s3://pixels-00");
        List<Status> statuses = storage.listStatus("s3://pixels-00");
        System.out.println(statuses.size());
        for (Status status : statuses)
        {
            System.out.println(status.getPath());
        }
    }

    @Test
    public void testGetPaths() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage("s3://pixels-00");
        List<String> paths = storage.listPaths("s3://pixels-00");
        if (paths == null)
        {
            System.out.println("null");
            return;
        }
        for (String path : paths)
        {
            System.out.println(path);
        }
    }

    @Test
    public void testS3Reader() throws IOException
    {
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(Storage.Scheme.s3, "pixels-01/object-4");
        CompletableFuture<ByteBuffer> future = reader.readAsync(0, 8);
        future.whenComplete((resp, err) ->
        {
            if (resp != null)
            {
                System.out.println(resp.getLong());
            }
            else
            {
                err.printStackTrace();
            }
        });
        future.join();
        reader.close();
    }

    @Test
    public void testEnclosure() throws InterruptedException
    {
        //byte[] bytes = new byte[100];
        //PixelsProto.RowGroupFooter footer = PixelsProto.RowGroupFooter.parseFrom(ByteBuffer.wrap(null));
        //System.out.println(footer);
        AtomicInteger integer = new AtomicInteger(0);
        for (int i = 0; i < 3; ++i)
        {
            String a = "" + i;
            int fi = i;
            Thread thread = new Thread(() ->
            {
                try
                {
                    Thread.sleep(500);
                    System.out.println("in: " + integer.get());
                    System.out.println("in: " + a + ", " + fi);
                    integer.addAndGet(10);
                    System.out.println("in: " + integer.get());
                    System.out.println("in: " + a + ", " + fi);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            });
            thread.start();
        }
        integer.set(5);
        System.out.println(integer.get());
        Thread.sleep(1000);
        System.out.println(integer.get());
    }
}
