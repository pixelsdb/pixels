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
package io.pixelsdb.pixels.storage.http.io;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

public class HttpOutputStream extends OutputStream
{
    private static final Logger logger = LogManager.getLogger(HttpOutputStream.class);

    /**
     * The underlying protocol of http stream.
     * Default value is http.
     */
    private static final String protocol = "http";

    /**
     * The maximum retry count.
     */
    private static final int MAX_RETRIES = Constants.MAX_STREAM_RETRY_COUNT;

    /**
     * The delay between two tries.
     */
    private static final long RETRY_DELAY_MS = Constants.STREAM_DELAY_MS;

    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * The uri of http stream.
     */
    private final String uri;

    /**
     * The temporary buffer used for storing the chunks.
     */
    private final byte[] buffer;

    /**
     * The position in the buffer.
     */
    private int bufferPosition;

    private final PendingResponseSet pendingResponseSet = new PendingResponseSet(3);

    private final AtomicInteger partId = new AtomicInteger(0);

    /**
     * The http client.
     */
    private final AsyncHttpClient httpClient;

    public HttpOutputStream(String host, int port, int bufferCapacity)
    {
        this.open = true;
        this.uri = this.protocol + "://" + host + ":" + port;
        this.buffer = new byte[bufferCapacity];
        this.bufferPosition = 0;
        this.httpClient = Dsl.asyncHttpClient();
    }

    /**
     * Write a byte array to the http output stream.
     *
     * @param b the byte array to write
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
        if (len == 0)
        {
            return;
        }
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
    public synchronized void flush() throws IOException
    {
        assertOpen();
        if (this.bufferPosition > 0)
        {
            flushBufferAndRewind();
        }
    }

    protected void flushBufferAndRewind()
    {
        logger.debug("Sending {} bytes to http stream", this.bufferPosition);
        byte[] content = Arrays.copyOfRange(this.buffer, 0, this.bufferPosition);
        this.bufferPosition = 0;
        sendContent(content);
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            if (this.bufferPosition > 0)
            {
                flush();
            }
            this.open = false;
            closeStreamReader();
            this.httpClient.close();
        }
    }

    private void sendContent(byte[] content)
    {
        int retry = 0;
        while (retry <= MAX_RETRIES)
        {
            try
            {
                Request req = httpClient.preparePost(this.uri)
                        .setBody(ByteBuffer.wrap(content))
                        .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                        .addHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(content.length))
                        .addHeader(HttpHeaderNames.CONNECTION, "keep-alive")
                        .build();
                this.pendingResponseSet.pendForResponse(partId.getAndIncrement(), httpClient.executeRequest(req));
                break;
            } catch (Exception e)
            {
                retry++;
                if (!(e.getCause() instanceof java.net.ConnectException || e.getCause() instanceof java.io.IOException))
                {
                    logger.error("Failed to send content after {} retries", retry, e);
                    break;
                }
                try
                {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException e1)
                {
                    logger.error("Retry interrupted", e1);
                    break;
                }
            }
        }
    }

    /**
     * Tell stream reader that this http stream closes.
     */
    private void closeStreamReader()
    {
        Request req = httpClient.preparePost(this.uri)
                .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                .addHeader(HttpHeaderNames.CONTENT_LENGTH, "0")
                .addHeader(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
                .build();
        int retry = 0;
        while (retry <= MAX_RETRIES)
        {
            try
            {
                pendingResponseSet.waitAllComplete();
                httpClient.executeRequest(req).get();
                break;
            } catch (Exception e)
            {
                retry++;
                if (!(e.getCause() instanceof java.net.ConnectException))
                {
                    logger.error("Failed to close http output stream after {} retries", retry, e);
                    break;
                }
                try
                {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException e1)
                {
                    logger.error("Retry interrupted", e1);
                    break;
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

    /**
     * The set to manage pending http future responses. It allows at most {@link #capacity()} asynchronous http
     * request to be executed concurrently and waits for their responses.
     */
    public static class PendingResponseSet
    {
        private final int capacity;

        private volatile int numPendingResponse;

        private volatile boolean shutdown;

        public PendingResponseSet(int capacity)
        {
            checkArgument(capacity > 0, "capacity must be greater than 0");
            this.capacity = capacity;
            this.numPendingResponse = 0;
            this.shutdown = false;
        }

        public synchronized void pendForResponse(int partId, ListenableFuture<Response> response) throws InterruptedException
        {
            if (shutdown)
            {
                throw new IllegalStateException("pending response set is already shutdown");
            }
            while (this.numPendingResponse >= capacity)
            {
                this.wait();
            }
            this.numPendingResponse++;
            response.addListener(new ResponseListener(partId, this), null);
        }

        /**
         * This method should be called from a thread other than the thread which called
         * {@link #pendForResponse(int, ListenableFuture)}.
         * @param partId the part id of the completed response.
         */
        public synchronized void completeResponse(int partId)
        {
            this.numPendingResponse--;
            this.notify();
        }

        public synchronized boolean isEmpty()
        {
            return this.numPendingResponse == 0;
        }

        public int capacity()
        {
            return this.capacity;
        }

        public synchronized void shutdown()
        {
            this.shutdown = true;
        }

        /**
         * Shutdown this {@link PendingResponseSet} and wait for the pending responses to complete.
         * This method is not synchronized, otherwise {@link #completeResponse(int)} can not be executed to clear the set.
         * @throws InterruptedException
         */
        public void waitAllComplete() throws InterruptedException
        {
            shutdown();
            // isEmpty is synchronized, so that it does not read the intermediate state of pendForResponse().
            while (!isEmpty())
            {
                // loop and wait for the pending responses to complete.
                Thread.sleep(1);
            }
        }
    }

    public static class ResponseListener implements Runnable
    {
        private final int partId;
        private final PendingResponseSet responseSet;

        public ResponseListener(int partId, PendingResponseSet responseSet)
        {
            this.partId = partId;
            this.responseSet = responseSet;
        }

        @Override
        public void run()
        {
            this.responseSet.completeResponse(this.partId);
        }
    }
}
