/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.storage.stream;

import io.pixelsdb.pixels.common.physical.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;

public class TestStream
{
    private volatile Exception readerException = null;
    private volatile Exception writerException = null;

    @Test
    public void testStorage() throws IOException
    {
        Storage stream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        InputStream fileInput = Files.newInputStream(Paths.get("/tmp/test1"));
        OutputStream outputStream = stream.create("stream:///localhost:29920", false, 4096);
        InputStream inputStream = stream.open("stream:///localhost:29920");
        OutputStream fileOutput = Files.newOutputStream(Paths.get("/tmp/test2"));
        IOUtils.copyBytes(fileInput, outputStream, 4096, true);
        IOUtils.copyBytes(inputStream, fileOutput, 4096, true);
    }

    /**
     * Occasionally, the physicalReader fails to read the desired length of the string, causing the test to fail,
     * with a probability of less than 1/20.
     * @throws IOException
     */
    @Test
    public void testPhysicalReaderAndWriter() throws IOException
    {
        Storage stream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        Thread readerThread = new Thread(() -> {
            try
            {
                try (PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(stream, "stream:///localhost:29920"))
                {
                    int num1 = fsReader.readInt(ByteOrder.BIG_ENDIAN);
                    assert(num1 == 13);
                    num1 = fsReader.readInt(ByteOrder.BIG_ENDIAN);
                    assert(num1 == 169);

                    long num2 = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                    assert(num2 == 28561);
                    num2 = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                    assert(num2 == 815730721);

                    ByteBuffer buffer;
                    for (int len = 1; len < 1000000000; len=len*2)
                    {
                        buffer = fsReader.readFully(len);
                        for (int i = 0; i < len; i++)
                        {
                            byte tmp = buffer.get();
                            if (tmp != 'a')
                            {
                                System.out.println(len);
                                throw new IOException("failed: " + len);
                            }
                        }
                    }
                }
            } catch (IOException e)
            {
                readerException = e;
                throw new RuntimeException(e);
            }
        });
        Thread writerThread = new Thread(() -> {
            try
            {
                try (PhysicalWriter fsWriter = PhysicalWriterUtil.newPhysicalWriter(stream, "stream:///localhost:29920", null))
                {
                    ByteBuffer buffer = ByteBuffer.allocate(24);
                    buffer.putInt(13);
                    buffer.putInt(169);
                    buffer.putLong(28561);
                    buffer.putLong(815730721);
                    fsWriter.append(buffer);
                    fsWriter.flush();
                    for (int len = 1; len < 1000000000; len=len*2)
                    {
                        buffer = ByteBuffer.allocate(len);
                        for (int i = 0; i < len; i++)
                        {
                            buffer.put((byte) 'a');
                        }
                        fsWriter.append(buffer);
                    }
                }
            } catch (IOException e)
            {
                writerException = e;
                throw new RuntimeException(e);
            }
        });
        readerThread.start();
        writerThread.start();
        try
        {
            readerThread.join();
            writerThread.join();
            if (this.readerException != null || this.writerException != null)
            {
                throw new IOException();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
