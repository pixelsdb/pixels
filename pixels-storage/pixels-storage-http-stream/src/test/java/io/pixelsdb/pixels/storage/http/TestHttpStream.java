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
package io.pixelsdb.pixels.storage.http;

import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.storage.http.io.HttpInputStream;
import io.pixelsdb.pixels.storage.http.io.HttpOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.util.Arrays;

public class TestHttpStream
{
    private volatile Exception readerException = null;
    private volatile Exception writerException = null;
    private final int sendLimit = 8*1024*1024;
    private final int sendNum = 1600;

    @Test
    public void testStorage() throws IOException
    {
        Storage httpStream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        InputStream fileInput = Files.newInputStream(Paths.get("/tmp/test1"));
        OutputStream outputStream = httpStream.create("httpstream://localhost:29920", false, 4096);
        InputStream inputStream = httpStream.open("httpstream://localhost:29920");
        OutputStream fileOutput = Files.newOutputStream(Paths.get("/tmp/test2"));
        IOUtils.copyBytes(fileInput, outputStream, 4096, true);
        IOUtils.copyBytes(inputStream, fileOutput, 4096, true);
    }

    @Test
    public void testPhysicalReaderAndWriter() throws IOException
    {
        Storage httpStream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        Thread readerThread = new Thread(() -> {
            try
            {
                try (PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(
                        httpStream, "httpstream://localhost:29920"))
                {
                    int num1 = fsReader.readInt(ByteOrder.BIG_ENDIAN);
                    assert(num1 == 13);
                    num1 = fsReader.readInt(ByteOrder.BIG_ENDIAN);
                    assert(num1 == 169);

                    long num2 = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                    assert(num2 == 28561L);
                    num2 = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                    assert(num2 == 815730721L);

                    boolean failed = false;
                    byte[] buffer = new byte[sendLimit];
                    for (int i = 0; i < sendNum; i++)
                    {
                        fsReader.readFully(buffer);
                        for (int j = 0; j < sendLimit; j++)
                        {
                            byte tmp = buffer[j];
                            if (tmp != (byte) ('a' + j % 10))
                            {
                                System.out.println("failed sendNum " + i + " sendLen " + sendLimit + " tmp: " + tmp);
                                failed = true;
                            }
                        }
                    }
                    if (failed)
                    {
                        throw new IOException("failed");
                    }
                }
            }
            catch (IOException e)
            {
                readerException = e;
                throw new RuntimeException(e);
            }
        });
        Thread writerThread = new Thread(() -> {
            try
            {
                try (PhysicalWriter fsWriter = PhysicalWriterUtil.newPhysicalWriter(
                        httpStream, "httpstream://localhost:29920"))
                {
                    ByteBuffer buffer = ByteBuffer.allocate(24);
                    buffer.putInt(13);
                    buffer.putInt(169);
                    buffer.putLong(28561L);
                    buffer.putLong(815730721L);
                    fsWriter.append(buffer);
                    fsWriter.flush();
                    buffer = ByteBuffer.allocate(sendLimit);
                    for (int j = 0; j < sendLimit; j++)
                    {
                        buffer.put((byte) ('a' + j % 10));
                    }
                    for (int i = 0; i < sendNum; i++)
                    {
                        fsWriter.append(buffer.array(), 0, sendLimit);
                        fsWriter.flush();
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            catch (IOException e)
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
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test whether data sent in groups can be completely received as different groups.
     * @throws IOException
     */
    @Test
    public void testPhysicalReaderAndWriter2() throws IOException
    {
        int[] lengths = {0, 1*1024*1024, 2*1024*1024, 4*1024*1024, 8*1024*1024, 16*1024*1024, 32*1024*1024, 64*1024*1024};
        byte[] contents = {'a', 'a', 'b', 'c', 'd', 'e', 'f'};
        Storage httpStream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        Thread readerThread = new Thread(() -> {
            try
            {
                try (PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(
                        httpStream, "httpstream://localhost:29920"))
                {
                    boolean failed = false;
                    for (int i = 0; i < sendNum; i++)
                    {
                        int len = fsReader.readInt(ByteOrder.BIG_ENDIAN);
                        ByteBuffer buffer;
                        buffer = fsReader.readFully(lengths[len]);
                        for (int j = 0; j < lengths[len]; j++)
                        {
                            byte tmp = buffer.get();
                            if (tmp != contents[len])
                            {
                                System.out.println("failed sendNum " + i + " sendLen " + len + " tmp: " + tmp);
                                failed = true;
                            }
                        }
                    }
                    Thread.sleep(30000);
                    if (failed)
                    {
                        throw new IOException("failed");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
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
                try (PhysicalWriter fsWriter = PhysicalWriterUtil.newPhysicalWriter(
                        httpStream, "httpstream://localhost:29920"))
                {
                    for (int i = 0; i < sendNum; i++)
                    {
                        int index = i%6 + 1;
                        ByteBuffer buffer = ByteBuffer.allocate(4);
                        buffer.order(ByteOrder.BIG_ENDIAN);
                        buffer.putInt(index);
                        fsWriter.append(buffer);
                        buffer = ByteBuffer.allocate(lengths[index]);
                        for (int j = 0; j < lengths[index]; j++)
                        {
                            buffer.put(contents[index]);
                        }
                        fsWriter.append(buffer);
                        fsWriter.flush();
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

    @Test
    public void testStream() throws IOException
    {
        Thread inputThread = new Thread(() -> {
            byte[] buffer = new byte[Constants.HTTP_STREAM_BUFFER_SIZE];
            try
            {
                try (HttpInputStream inputStream = new HttpInputStream("localhost", 29920))
                {
                    for (int i = 0; i < sendNum; i++)
                    {
                        inputStream.read(buffer);
                        for (int j = 0; j < Constants.HTTP_STREAM_BUFFER_SIZE; j++) {
                            if (buffer[j] != 'a')
                            {
                                System.out.println("failed sendNum " + i + " char " + buffer[j]);
                                readerException = new IOException();
                            }
                        }
                    }
                } catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            } catch (CertificateException e)
            {
                throw new RuntimeException(e);
            }
        });
        Thread outputThread = new Thread(() -> {
            byte[] buffer = new byte[Constants.HTTP_STREAM_BUFFER_SIZE];
            Arrays.fill(buffer, (byte) 'a');

            try (HttpOutputStream outputStream = new HttpOutputStream(
                    "localhost", 29920, Constants.HTTP_STREAM_BUFFER_SIZE))
            {
                for (int i = 0; i < sendNum; i++)
                {
                    outputStream.write(buffer);
                }
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
        inputThread.start();
        outputThread.start();

        try
        {
            inputThread.join();
            outputThread.join();
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        if (readerException != null)
        {
            throw new IOException();
        }
    }

    @Test
    public void testDataStream() throws IOException
    {
        Storage httpStream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        Thread inputThread = new Thread(() -> {
            byte[] buffer = new byte[Constants.HTTP_STREAM_BUFFER_SIZE];
            try (DataInputStream inputStream = httpStream.open("httpstream://localhost:29920"))
            {
                for (int i = 0; i < sendNum; i++)
                {
                    inputStream.read(buffer);
                    for (int j = 0; j < Constants.HTTP_STREAM_BUFFER_SIZE; j++) {
                        if (buffer[j] != 'a')
                        {
                            System.out.println("failed sendNum " + i + " sendLen " + buffer[j] + " tmp " + buffer[j]);
                            readerException = new IOException();
                        }
                    }
                }
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
        Thread outputThread = new Thread(() -> {
            byte[] buffer = new byte[Constants.HTTP_STREAM_BUFFER_SIZE];
            for (int i = 0; i < Constants.HTTP_STREAM_BUFFER_SIZE; i++)
            {
                buffer[i] = 'a';
            }

            try (DataOutputStream outputStream = httpStream.create(
                    "httpstream://localhost:29920", false, Constants.HTTP_STREAM_BUFFER_SIZE))
            {
                for (int i = 0; i < sendNum; i++)
                {
                    outputStream.write(buffer);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
        inputThread.start();
        outputThread.start();

        try
        {
            inputThread.join();
            outputThread.join();
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        if (readerException != null)
        {
            throw new IOException();
        }
    }
}
