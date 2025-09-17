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
import static io.pixelsdb.pixels.storage.http.io.HttpContentQueue.PART_ID;

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
     * The number of concurrent http requests that are waiting for responses.
     */
    private static final int NUM_CONCURRENT_REQUESTS = 5;

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

    private final RunningRequestSet runningRequestSet = new RunningRequestSet(NUM_CONCURRENT_REQUESTS);

    /**
     * valid part id starts from 0.
     */
    private final AtomicInteger currPartId = new AtomicInteger(0);

    /**
     * The http client.
     */
    private final AsyncHttpClient httpClient;

    public HttpOutputStream(String host, int port, int bufferCapacity)
    {
        this.open = true;
        this.uri = protocol + "://" + host + ":" + port;
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
        int partId = this.currPartId.getAndIncrement();
        sendContent(new ContentRequest(partId, content));
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

    private void sendContent(ContentRequest request)
    {
        int retry = 0;
        while (retry <= MAX_RETRIES)
        {
            try
            {
                byte[] content = request.getContent();
                Request req = httpClient.preparePost(this.uri)
                        .setBody(ByteBuffer.wrap(content))
                        .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                        .addHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(content.length))
                        .addHeader(HttpHeaderNames.CONNECTION, "keep-alive")
                        .addHeader(PART_ID, request.partId)
                        .build();
                this.runningRequestSet.add(request, httpClient.executeRequest(req));
                break;
            }
            catch (Exception e)
            {
                retry++;
                if (!(e.getCause() instanceof java.net.ConnectException || e.getCause() instanceof java.io.IOException))
                {
                    logger.error("failed to send content after {} retries", retry, e);
                    break;
                }
                try
                {
                    Thread.sleep(RETRY_DELAY_MS);
                }
                catch (InterruptedException e1)
                {
                    logger.error("retry interrupted", e1);
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
        int retries = 0;
        while (retries <= MAX_RETRIES)
        {
            try
            {
                runningRequestSet.waitAllComplete();
                httpClient.executeRequest(req).get();
                break;
            }
            catch (Exception e)
            {
                retries++;
                if (!(e.getCause() instanceof java.net.ConnectException))
                {
                    logger.error("Failed to close http output stream after {} retries", retries, e);
                    break;
                }
                try
                {
                    Thread.sleep(RETRY_DELAY_MS);
                }
                catch (InterruptedException e1)
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
     * The set to manage concurrent running http requests. It allows at most {@link #capacity()} asynchronous http
     * requests to be executed concurrently and waits for their responses.
     */
    public class RunningRequestSet
    {
        private final int capacity;

        private volatile int numRunningRequests;

        private volatile boolean shutdown;

        public RunningRequestSet(int capacity)
        {
            checkArgument(capacity > 0, "capacity must be greater than 0");
            this.capacity = capacity;
            this.numRunningRequests = 0;
            this.shutdown = false;
        }

        public synchronized void add(ContentRequest request, ListenableFuture<Response> response) throws InterruptedException
        {
            if (shutdown)
            {
                throw new IllegalStateException("pending response set is already shutdown");
            }
            while (this.numRunningRequests >= capacity)
            {
                this.wait();
            }
            this.numRunningRequests++;
            response.toCompletableFuture().whenComplete((resp, err) -> {
                if (err != null)
                {
                    if (request.getRetries() < MAX_RETRIES)
                    {
                        logger.error("failed to send content {} and retrying", request.partId, err);
                        request.incrementReties();
                        sendContent(request);
                    }
                    else
                    {
                        logger.error("failed to send content {} after {} retries",
                                request.partId, request.getRetries(), err);
                        complete();
                    }
                }
                else
                {
                    complete();
                }
            });
        }

        /**
         * This method should be called from a thread other than the thread which called
         * {@link #add(ContentRequest, ListenableFuture)}.
         */
        public synchronized void complete()
        {
            this.numRunningRequests--;
            this.notify();
        }

        public synchronized boolean isEmpty()
        {
            return this.numRunningRequests == 0;
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
         * Shutdown this {@link RunningRequestSet} and wait for the pending responses to complete.
         * This method is not synchronized, otherwise {@link #complete()} can not be executed to clear the set.
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

    public static class ContentRequest
    {
        private final int partId;
        private final byte[] content;
        private final AtomicInteger retries;

        public ContentRequest(int partId, byte[] content)
        {
            this.partId = partId;
            this.content = content;
            this.retries = new AtomicInteger(0);
        }

        public int getPartId()
        {
            return partId;
        }

        public byte[] getContent()
        {
            return content;
        }

        public int getRetries()
        {
            return this.retries.get();
        }

        public void incrementReties()
        {
            this.retries.incrementAndGet();
        }
    }
}
