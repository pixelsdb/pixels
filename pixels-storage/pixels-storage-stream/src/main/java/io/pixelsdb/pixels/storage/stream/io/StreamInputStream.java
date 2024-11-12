package io.pixelsdb.pixels.storage.stream.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.pixelsdb.pixels.common.utils.HttpServer;
import io.pixelsdb.pixels.common.utils.HttpServerHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.concurrent.*;

public class StreamInputStream extends InputStream
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
     * The temporary buffer used for storing the chunks.
     */
    private final BlockingQueue<ByteBuf> contentQueue;

    /**
     * The capacity of buffer.
     */
    private final int bufferCapacity = 1000000000;

    /**
     * The maximum tries to get data.
     */
    private final int MAX_TRIES = 10;

    /**
     * The milliseconds to sleep.
     */
    private final int DELAY_MS = 2000;

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

    public StreamInputStream(String host, int port) throws CertificateException, SSLException {
        this.open = true;
        this.contentQueue = new LinkedBlockingDeque<>();
        this.host = host;
        this.port = port;
        this.uri = this.schema + "://" + host + ":" + port;
        this.httpServer = new HttpServer(new StreamHttpServerHandler(this));
        this.executorService = Executors.newFixedThreadPool(1);
        this.httpServerFuture = CompletableFuture.runAsync(() -> {
            try
            {
                this.httpServer.serve(this.port);
            } catch (InterruptedException e)
            {
                logger.error("http server interrupted", e);
            }
        }, this.executorService);
    }

    @Override
    public int read() throws IOException
    {
        assertOpen();
        if (!assertData())
        {
            return -1;
        }

        ByteBuf content = this.contentQueue.peek();
        int b = -1;
        if (content != null)
        {
            b = content.readUnsignedByte();
            if (!content.isReadable())
            {
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
     * Attempt to read data with a maximum length of len into the position off of buf.
     * @param buf
     * @param off
     * @param len
     * @return Actual number of bytes read
     * @throws IOException
     */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException
    {
        assertOpen();
        if (!assertData())
        {
            return -1;
        }

        int readBytes = 0;
        while (readBytes < len && !this.contentQueue.isEmpty())
        {
            ByteBuf content = this.contentQueue.peek();
            int readLen = Math.min(len-readBytes, content.readableBytes());
            content.readBytes(buf, readBytes, readLen);
            if (!content.isReadable())
            {
                content.release();
                this.contentQueue.poll();
            }
            readBytes += readLen;
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
        }
    }

    private boolean assertData() throws IOException
    {
        int tries = 0;
        while (tries < this.MAX_TRIES && this.contentQueue.isEmpty() && !this.httpServerFuture.isDone())
        {
            try
            {
                tries++;
                Thread.sleep(this.DELAY_MS);
            } catch (InterruptedException e)
            {
                throw new IOException(e);
            }
        }

        return !this.contentQueue.isEmpty();
    }

    private void assertOpen()
    {
        if (!this.open)
        {
            throw new IllegalStateException("Closed");
        }
    }

    public static class StreamHttpServerHandler extends HttpServerHandler
    {
        private static final Logger logger = LogManager.getLogger(StreamHttpServerHandler.class);
        private StreamInputStream inputStream;

        public StreamHttpServerHandler(StreamInputStream inputStream)
        {
            this.inputStream = inputStream;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg)
        {
            if (!(msg instanceof HttpRequest))
            {
                return;
            }
            FullHttpRequest req = (FullHttpRequest) msg;
            if (req.method() != HttpMethod.POST)
            {
                req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
                sendResponse(ctx, req, HttpResponseStatus.BAD_REQUEST);
            }

            if (!req.headers().get(HttpHeaderNames.CONTENT_TYPE).equals("application/x-protobuf"))
            {
                return;
            }
            ByteBuf content = req.content();
            if (content.isReadable())
            {
                content.retain();
                this.inputStream.contentQueue.add(content);
            }
            sendResponse(ctx, req, HttpResponseStatus.OK);
        }

        private void sendResponse(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status)
        {
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), status);
            response.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                    .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

            if (req.headers().get(HttpHeaderNames.CONNECTION).equals(HttpHeaderValues.CLOSE.toString()))
            {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
                response.setStatus(status);
                ctx.writeAndFlush(response);
                this.serverCloser.run();
            } else
            {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                ctx.writeAndFlush(response);
            }
        }
    }
}
