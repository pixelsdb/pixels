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

import io.netty.buffer.ByteBuf;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpInputStream extends InputStream
{
    private static final Logger logger = LogManager.getLogger(HttpInputStream.class);

    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * The temporary buffer used for storing the chunks.
     */
    private final HttpContentQueue contentQueue;

    /**
     * The maximum tries to get data.
     */
    private final int MAX_TRIES = Constants.MAX_STREAM_RETRY_COUNT;

    /**
     * The http server for receiving input stream.
     */
    private final HttpServer httpServer;

    /**
     * The thread to run http server.
     */
    private final ExecutorService executorService;

    /**
     * The future of http server.
     */
    private final CompletableFuture<Void> httpServerFuture;

    public HttpInputStream(String host, int port) throws CertificateException, SSLException
    {
        this.open = true;
        this.contentQueue = new HttpContentQueue();
        this.httpServer = new HttpServer(new HttpServerHandler(this.contentQueue));
        this.executorService = Executors.newFixedThreadPool(1);
        this.httpServerFuture = CompletableFuture.runAsync(() -> {
            try
            {
                this.httpServer.serve(host, port);
            }
            catch (InterruptedException e)
            {
                logger.error("http server interrupted", e);
            }
            catch (UnknownHostException e)
            {
                logger.error("unknown host for http server", e);
            }
        }, this.executorService);
    }

    @Override
    public int read() throws IOException
    {
        assertOpen();
        if (emptyData())
        {
            return -1;
        }

        HttpContent content = this.contentQueue.peek();
        int b = -1;
        if (content != null)
        {
            ByteBuf contentBuf = content.getContent();
            b = contentBuf.readUnsignedByte();
            if (!contentBuf.isReadable())
            {
                contentBuf.release();
                this.contentQueue.poll();
            }
        }
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    /**
     * Attempt to read data with a maximum length of len into the position off of the buffer.
     * @param buf the buffer
     * @param off the position in buffer
     * @param len the length in bytes to read
     * @return actual number of bytes read
     * @throws IOException
     */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException
    {
        assertOpen();

        HttpContent content;
        int readBytes = 0;
        while (readBytes < len)
        {
            if (emptyData())
            {
                return readBytes > 0 ? readBytes : -1;
            }
            content = this.contentQueue.peek();
            if (content == null)
            {
                return readBytes > 0 ? readBytes : -1;
            }
            ByteBuf contentBuf = content.getContent();
            try
            {
                int readLen = Math.min(len - readBytes, contentBuf.readableBytes());
                contentBuf.readBytes(buf, off + readBytes, readLen);
                readBytes += readLen;
                if (!contentBuf.isReadable())
                {
                    contentQueue.poll();
                    contentBuf.release();
                }
            }
            catch (Exception e)
            {
                if (!contentBuf.isReadable())
                {
                    contentQueue.poll();
                    contentBuf.release();
                }
                throw e;
            }
        }

        return readBytes;
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            this.open = false;
            this.httpServerFuture.complete(null);
            this.httpServer.close();
            this.executorService.shutdown();
            this.contentQueue.clear();
        }
    }

    /**
     * Check if the content queue is empty. This method waits for the content queue to be none-empty for at most
     * {@link #MAX_TRIES} seconds.
     * @return true if the content queue is still empty after the waiting period.
     * @throws IOException
     */
    private boolean emptyData() throws IOException
    {
        int tries = 0;
        while (tries < this.MAX_TRIES && this.contentQueue.isEmpty() && !this.httpServerFuture.isDone())
        {
            try
            {
                tries++;
                Thread.sleep(Constants.STREAM_DELAY_MS);
            }
            catch (InterruptedException e)
            {
                throw new IOException(e);
            }
        }
        if (tries == this.MAX_TRIES)
        {
            logger.error("retry count {}, httpServerFuture {}, " +
                    "exception cause: HttpInputStream failed to receive data", tries, this.httpServerFuture.isDone());
        }

        return this.contentQueue.isEmpty();
    }

    private void assertOpen()
    {
        if (!this.open)
        {
            throw new IllegalStateException("http input stream is closed");
        }
    }
}
