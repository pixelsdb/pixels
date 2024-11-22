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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.storage.stream.io.StreamInputStream;
import io.pixelsdb.pixels.storage.stream.io.StreamOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;

public class TestStream
{
    private volatile Exception readerException = null;
    private volatile Exception writerException = null;
    private final int sendLimit = 8*1024*1024;
    private final int sendNum = 1600;

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

                    boolean failed = false;
                    for (int i = 0; i < sendNum; i++)
                    {
                        for (int len = sendLimit; len <= sendLimit; len=len*2)
                        {
                            buffer = fsReader.readFully(len);
                            for (int j = 0; j < len; j++)
                            {
                                byte tmp = buffer.get();
                                if (tmp != (byte) ('a'+j%10))
                                {
                                    System.out.println("failed sendNum " + i + " sendLen " + len + " tmp: " + tmp);
                                    failed = true;
                                }
                            }
                        }
                    }
                    if (failed)
                    {
                        throw new IOException("failed");
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
                    for (int i = 0; i < sendNum; i++)
                    {
                        for (int len = sendLimit; len <= sendLimit; len=len*2)
                        {
                            buffer = ByteBuffer.allocate(len+1);
                            for (int j = 0; j < len; j++)
                            {
                                buffer.put((byte) ('a'+j%10));
                            }
                            fsWriter.append(buffer);
                            fsWriter.flush();
                        }
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
            byte[] buffer = new byte[Constants.STREAM_BUFFER_SIZE];
            try
            {
                try (StreamInputStream inputStream = new StreamInputStream("localhost", 29920))
                {
                    for (int i = 0; i < sendNum; i++)
                    {
                        inputStream.read(buffer);
                        for (int j = 0; j < Constants.STREAM_BUFFER_SIZE; j++) {
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
            byte[] buffer = new byte[Constants.STREAM_BUFFER_SIZE];
            for (int i = 0; i < Constants.STREAM_BUFFER_SIZE; i++)
            {
                buffer[i] = 'a';
            }

            try (StreamOutputStream outputStream = new StreamOutputStream("localhost", 29920, Constants.STREAM_BUFFER_SIZE))
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
        Storage stream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        Thread inputThread = new Thread(() -> {
            byte[] buffer = new byte[Constants.STREAM_BUFFER_SIZE];
            try (DataInputStream inputStream = stream.open("stream:///localhost:29920"))
            {
                for (int i = 0; i < sendNum; i++)
                {
                    inputStream.read(buffer);
                    for (int j = 0; j < Constants.STREAM_BUFFER_SIZE; j++) {
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
            byte[] buffer = new byte[Constants.STREAM_BUFFER_SIZE];
            for (int i = 0; i < Constants.STREAM_BUFFER_SIZE; i++)
            {
                buffer[i] = 'a';
            }

            try (DataOutputStream outputStream = stream.create("stream:///localhost:29920", false, Constants.STREAM_BUFFER_SIZE))
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
