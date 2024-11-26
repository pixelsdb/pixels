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
package io.pixelsdb.pixels.storage.stream.io;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class StreamOutputStream extends OutputStream
{
    private static final Logger logger = LogManager.getLogger(StreamInputStream.class);

    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * The schema of http stream.
     * Default value is http.
     */
    private final String schema = "http";

    /**
     * The host of http stream.
     */
    private String host;

    /**
     * The port of http stream.
     */
    private int port;

    /**
     * The uri of http stream.
     */
    private String uri;

    /**
     * The maximum retry count.
     */
    private static final int MAX_RETRIES = 10;

    /**
     * The delay between two tries.
     */
    private static final long RETRY_DELAY_MS = 1000;

    /**
     * The temporary buffer used for storing the chunks.
     */
    private final byte[] buffer;

    /**
     * The position in the buffer.
     */
    private int bufferPosition;

    /**
     * The capacity of buffer.
     */
    private int bufferCapacity;

    /**
     * The http client.
     */
    private final AsyncHttpClient httpClient;

    public StreamOutputStream(String host, int port, int bufferCapacity)
    {
        this.open = true;
        this.host = host;
        this.port = port;
        this.uri = this.schema + "://" + host + ":" + port;
        this.bufferCapacity = bufferCapacity;
        this.buffer = new byte[bufferCapacity];
        this.bufferPosition = 0;
        this.httpClient = Dsl.asyncHttpClient();
    }

    /**
     * Write an array to the S3 output stream
     *
     * @param b
     * @throws IOException
     */
    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] buf, final int off, final int len) throws IOException
    {
        this.assertOpen();
        int offsetInBuf = off, remainToRead = len;
        int remainInBuffer;
        while (remainToRead > (remainInBuffer = this.buffer.length - bufferPosition))
        {
            System.arraycopy(buf, offsetInBuf, this.buffer, this.bufferPosition, remainInBuffer);
            this.bufferPosition += remainInBuffer;
            flushBufferAndRewind();
            offsetInBuf += remainInBuffer;
            remainToRead -= remainInBuffer;
        }
        System.arraycopy(buf, offsetInBuf, this.buffer, this.bufferPosition, remainToRead);
        this.bufferPosition += remainToRead;
    }

    @Override
    public void write(int b) throws IOException
    {
        this.assertOpen();
        if (this.bufferPosition >= this.buffer.length)
        {
            flushBufferAndRewind();
        }
        this.buffer[this.bufferPosition++] = (byte) b;
    }

    @Override
    public synchronized void flush()
    {
        assertOpen();
        try
        {
            flushBufferAndRewind();
        } catch (IOException e)
        {
            logger.error(e);
        }
    }

    protected void flushBufferAndRewind() throws IOException
    {
        logger.debug("Sending {} bytes to stream", this.bufferPosition);
        Request req = httpClient.preparePost(this.uri)
                .setBody(ByteBuffer.wrap(this.buffer, 0, this.bufferPosition))
                .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                .addHeader(HttpHeaderNames.CONTENT_LENGTH, this.bufferPosition)
                .addHeader(HttpHeaderNames.CONNECTION, "keep-aliva")
                .build();
        int retry = 0;
        while (true)
        {
            StreamHttpClientHandler handler = new StreamHttpClientHandler();
            try
            {
                httpClient.executeRequest(req, handler).get();
                this.bufferPosition = 0;
                break;
            } catch (Exception e)
            {
                retry++;
                if (retry > MAX_RETRIES || !(e.getCause() instanceof java.net.ConnectException))
                {
                    logger.error("retry count {}, exception cause {}, excepetion {}", retry, e.getCause(), e.getMessage());
                    throw new IOException("Connect to stream failed");
                } else
                {
                    try
                    {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException e1)
                    {
                        throw new IOException(e1);
                    }
                }
            }
        }
        this.bufferPosition = 0;
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            this.open = false;
            if (this.bufferPosition > 0)
            {
                flushBufferAndRewind();
            }
            closeStreamReader();
            this.httpClient.close();
        }
    }

    /**
     * Tell stream reader that this stream closes.
     */
    private void closeStreamReader()
    {
        Request req = httpClient.preparePost(this.uri)
                .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                .addHeader(HttpHeaderNames.CONTENT_LENGTH, 0)
                .addHeader(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
                .build();
        int retry = 0;
        while (true)
        {
            StreamHttpClientHandler handler = new StreamHttpClientHandler();
            try
            {
                httpClient.executeRequest(req, handler).get();
                break;
            } catch (Exception e)
            {
                retry++;
                if (retry > this.MAX_RETRIES || !(e.getCause() instanceof java.net.ConnectException))
                {
                    logger.error("failed to close stream reader");
                }
            }
        }
    }

    private void assertOpen()
    {
        if (!this.open)
        {
            throw new IllegalStateException("Closed");
        }
    }

    public static class StreamHttpClientHandler extends AsyncCompletionHandler<Response>
    {
        @Override
        public Response onCompleted(Response response) throws Exception
        {
            if (response.getStatusCode() != 200)
            {
                throw new IOException("Failed to send package to server, status code: " + response.getStatusCode());
            }
            return response;
        }

        @Override
        public void onThrowable(Throwable t)
        {
            logger.error("stream http client handler, {}", t.getMessage());
        }
    }
}
