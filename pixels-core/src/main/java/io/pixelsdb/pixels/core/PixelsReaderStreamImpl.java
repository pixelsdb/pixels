/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.core;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.HttpServer;
import io.pixelsdb.pixels.common.utils.HttpServerHandler;
import io.pixelsdb.pixels.core.exception.PixelsFileMagicInvalidException;
import io.pixelsdb.pixels.core.exception.PixelsFileVersionInvalidException;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.reader.PixelsRecordReaderStreamImpl;
import io.pixelsdb.pixels.core.utils.BlockingMap;
import io.pixelsdb.pixels.turbo.StreamProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.pixelsdb.pixels.common.utils.Constants.MAGIC;

/**
 * PixelsReaderStreamImpl is an implementation of {@link io.pixelsdb.pixels.core.PixelsReader} that reads
 *  ColumnChunks from a stream, for operator pipelining over HTTP.
 * DESIGN: We adopt the stream protocol defined in the head comment in {@link PixelsWriterStreamImpl}.
 *  In the stream reader, we use a shared queue, or in partitioned mode a blocking hash map (which maps hash value
 *  to the ByteBuf of the corresponding hash partition), to pass received `ByteBuf`s to the record stream reader.
 * <p>
 * TODO: Currently, we assume the HTTP messages arrive in order. Implement a state machine to handle out-of-order
 *  messages (e.g. send a response to the client to ask for retransmission, if the header packet has not arrived
 *  by the time a data packet arrives).
 */
@NotThreadSafe
public class PixelsReaderStreamImpl implements PixelsReader
{
    private static final Logger logger = LogManager.getLogger(PixelsReaderStreamImpl.class);
    /**
     * The number of bytes that the start offset of each column chunk is aligned to.
     */
    private static final int CHUNK_ALIGNMENT = Integer.parseInt(ConfigFactory.Instance()
            .getProperty("column.chunk.alignment"));

    private TypeDescription fileSchema;
    private final HttpServer httpServer;
    private final CompletableFuture<Void> httpServerFuture;
    private final BlockingQueue<ByteBuf> byteBufSharedQueue;
    // In partitioned mode, we use byteBufBlockingMap to map hash value to corresponding ByteBuf
    private final BlockingMap<Integer, ByteBuf> byteBufBlockingMap;
    private final boolean partitioned;
    private final AtomicReference<Integer> numPartitionsReceived = new AtomicReference<>(0);
    private final List<PixelsRecordReaderStreamImpl> recordReaders;

    /**
     * The streamHeader is in the first message received on the stream, containing the schema of the file.
     * It is used to initialize the fileSchema and the recordReaders.
     * It is set to null until the first message arrives.
     * The streamHeaderLatch is used to wait for the streamHeader to arrive.
     */
    private StreamProto.StreamHeader streamHeader;
    private final CountDownLatch streamHeaderLatch = new CountDownLatch(1);

    public PixelsReaderStreamImpl(String endpoint) throws Exception
    {
        this(endpoint, false, -1);
    }

    public PixelsReaderStreamImpl(int port) throws Exception
    {
        this("http://localhost:" + port + "/");
    }

    public PixelsReaderStreamImpl(String endpoint, boolean partitioned, int numPartitions)
            throws URISyntaxException, CertificateException, SSLException
    {
        this.fileSchema = null;
        this.streamHeader = null;
        URI uri = new URI(endpoint);
        String IP = uri.getHost();
        int httpPort = uri.getPort();
        logger.debug("In Pixels stream reader constructor, IP: " + IP + ", port: " + httpPort +
                ", partitioned: " + partitioned + ", numPartitions: " + numPartitions);
        if (!Objects.equals(IP, "127.0.0.1") && !Objects.equals(IP, "localhost"))
        {
            throw new UnsupportedOperationException("Currently, only localhost is supported as the server address");
        }
        this.byteBufSharedQueue = new LinkedBlockingQueue<>(1);
        this.byteBufBlockingMap = new BlockingMap<>();
        this.partitioned = partitioned;
        this.recordReaders = new ArrayList<>();

        // WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
        ExecutorService executorService = Executors.newFixedThreadPool(1);  // , new ThreadFactoryBuilder()
        // .setUncaughtExceptionHandler(exceptionHandler).build());
        this.httpServer = new HttpServer(new HttpServerHandler()
        {
            @Override
            public void channelRead0(ChannelHandlerContext ctx, HttpObject msg)
            {
                // Concurrency: async thread. Reads or writes streamHeader, recordReaders, byteBufSharedQueue
                if (!(msg instanceof HttpRequest)) return;
                FullHttpRequest req = (FullHttpRequest) msg;
                if (req.method() != HttpMethod.POST)
                {
                    sendResponseAndClose(ctx, req, NOT_FOUND);
                    return;
                }
                if (!Objects.equals(req.headers().get("Content-Type"), "application/x-protobuf"))
                {
                    // silent reject
                    return;
                }
                logger.debug("Incoming packet on port: " + httpPort +
                        ", content_length header: " + req.headers().get("content-length") +
                        ", connection header: " + req.headers().get("connection") +
                        ", partition ID header: " + req.headers().get("X-Partition-Id") +
                        ", HTTP request object body total length: " + req.content().readableBytes());

                ByteBuf byteBuf = req.content();
                try
                {
                    if (streamHeader == null)
                    {
                        try
                        {
                            streamHeader = parseStreamHeader(byteBuf);
                            streamHeaderLatch.countDown();

                            for (PixelsRecordReaderStreamImpl recordReader : recordReaders)
                            {
                                // XXX: potential data race if `read()` method and this handler are executed in parallel
                                //  due to concurrent modifications of the `recordReaders` list
                                recordReader.lateInitialization(streamHeader);
                            }
                        } catch (IOException e)
                        {
                            logger.error("Invalid stream header values: ", e);
                            sendResponseAndClose(ctx, req, BAD_REQUEST);
                            return;
                        }
                    } else if (partitioned)
                    {
                        // In partitioned mode, every packet brings a streamHeader to prevent errors from possibly
                        // out-of-order packet arrivals, so we need to parse it, but do not need the return value
                        // (except for the first incoming packet processed above).
                        parseStreamHeader(byteBuf);
                    }
                } catch (InvalidProtocolBufferException | IndexOutOfBoundsException e)
                {
                    logger.error("Malformed or corrupted stream header", e);
                    sendResponseAndClose(ctx, req, BAD_REQUEST);
                    return;
                }

                // We only need to put the byteBuf into the blocking queue to pass it to the recordReader, if the
                //  client is a data writer (port >= 50100) rather than a schema writer. In the latter case,
                //  the schema packet has been processed when parsing the stream header above.
                if (httpPort >= 50100)
                {
                    // We use an ExecutorService to put the ByteBuf into the blocking queue, to avoid blocking the
                    //  Netty event loop thread.
                    // We can also use a group of partition-aware handlers in the Netty pipeline, to put the ByteBuf
                    //  into the blocking map, instead of using an ExecutorService. But that would be more complex.
                    CompletableFuture.runAsync(() -> {
                        try
                        {
                            byteBuf.retain();
                            if (!partitioned) byteBufSharedQueue.put(byteBuf);
                            else
                            {
                                int partitionId = Integer.parseInt(req.headers().get("X-Partition-Id"));
                                if (partitionId < 0 || partitionId >= numPartitions)
                                {
                                    logger.warn("Client sent invalid partitionId value: " + partitionId);
                                    sendResponseAndClose(ctx, req, BAD_REQUEST);
                                    return;
                                }
                                byteBufBlockingMap.put(partitionId, byteBuf);
                                if (numPartitionsReceived.accumulateAndGet(1, Integer::sum) == numPartitions)
                                {
                                    // The reader has read all the partitions, so we can put an artificial empty ByteBuf
                                    //  into the queue to signal the end of the stream.
                                    byteBufBlockingMap.put(numPartitions, Unpooled.buffer(0).retain());
                                }
                            }
                        } catch (InterruptedException e)
                        {
                            logger.error("Interrupted while putting ByteBuf into blocking queue", e);
                            sendResponseAndClose(ctx, req, INTERNAL_SERVER_ERROR);
                            // 只有用户用终端（c.f. pixels-cli）时可以e.printStackTrace()
                        }
                    });
                }

                sendResponseAndClose(ctx, req, HttpResponseStatus.OK);
            }

            private void sendResponseAndClose(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status)
            {
                FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), status);
                response.headers()
                        .set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                        .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
                        .set(CONNECTION, CLOSE);

                ChannelFuture f = ctx.writeAndFlush(response);
                f.addListener(future -> {
                    if (!future.isSuccess())
                    {
                        logger.error("Failed to write response: " + future.cause());
                        ctx.channel().close();
                    }
                });
                f.addListener(ChannelFutureListener.CLOSE);  // shut down the connection with the current hash
                if (Objects.equals(req.headers().get(CONNECTION), CLOSE.toString()) ||
                        (partitioned && numPartitionsReceived.get() == numPartitions))
                {
                    f.addListener(future -> {
                        // shutdown the server
                        ctx.channel().parent().close().addListener(ChannelFutureListener.CLOSE);
                        // removes schema port to avoid port conflict or misuse
                        // if (httpPort < 50100)
                        //     StreamWorkerCommon.delPort(httpPort);
                    });
                }
            }
        });
        this.httpServerFuture = CompletableFuture.runAsync(() -> {
            try
            {
                this.httpServer.serve(httpPort);
            } catch (InterruptedException e)
            {
                logger.error("HTTP server interrupted", e);
            }
        }, executorService);
    }

    static int calculateCeiling(int value, int multiple)
    {
        // to calculate padding length in HttpClient

        if (value <= 0 || multiple <= 0)
        {
            throw new IllegalArgumentException("Both value and multiple must be positive.");
        }

        int remainder = value % multiple;
        if (remainder == 0)
        {
            return value;
        }

        int difference = multiple - remainder;
        return value + difference;
    }

    private StreamProto.StreamHeader parseStreamHeader(ByteBuf byteBuf)
            throws InvalidProtocolBufferException, IndexOutOfBoundsException
    {
        // check MAGIC
        int magicLength = MAGIC.getBytes().length;
        byte[] magicBytes = new byte[magicLength];
        byteBuf.getBytes(0, magicBytes);
        String magic = new String(magicBytes);
        if (!magic.contentEquals(Constants.MAGIC))
        {
            throw new PixelsFileMagicInvalidException(magic);
        }

        int metadataLength = byteBuf.getInt(magicLength);
        ByteBuf metadataBuf = Unpooled.buffer(metadataLength);
        byteBuf.getBytes(magicLength + Integer.BYTES, metadataBuf);
        StreamProto.StreamHeader streamHeader = StreamProto.StreamHeader.parseFrom(metadataBuf.nioBuffer());

        // check file version
        int fileVersion = streamHeader.getVersion();
        if (!PixelsVersion.matchVersion(fileVersion))
        {
            throw new PixelsFileVersionInvalidException(fileVersion);
        }

        // consume the padding bytes
        byteBuf.readerIndex(calculateCeiling(magicLength + Integer.BYTES + metadataLength, 8));
        if (CHUNK_ALIGNMENT != 0)
            byteBuf.readerIndex(calculateCeiling(magicLength + Integer.BYTES + metadataLength, CHUNK_ALIGNMENT));
        // At this point, the readerIndex of the byteBuf is past the streamHeader and at the start of
        // the actual rowGroups.

        this.fileSchema = TypeDescription.createSchema(streamHeader.getTypesList());
        return streamHeader;
    }

    public PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId)
    {
        throw new UnsupportedOperationException("getRowGroupFooter is not supported in a stream");
    }

    /**
     * Get a <code>PixelsRecordReader</code>
     * Currently under streaming mode, only 1 recordReader per Reader.
     * todo: implement multi-thread read in the future
     *  (careful - the byteBuf in the HTTP serve method will possibly be shared in that case)
     *
     * @return record reader
     */
    @Override
    public PixelsRecordReader read(PixelsReaderOption option) throws IOException
    {
        assert (recordReaders.size() == 0);

        PixelsRecordReaderStreamImpl recordReader = new PixelsRecordReaderStreamImpl(partitioned, byteBufSharedQueue,
                byteBufBlockingMap, streamHeader, option);
        recordReaders.add(recordReader);
        return recordReader;
    }

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    @Override
    public PixelsVersion getFileVersion()
    {
        return PixelsVersion.from(this.streamHeader.getVersion());
    }

    /**
     * Unsupported: In streaming mode, the number of rows cannot be determined in advance.
     */
    // 用到numberOfRows的有三种情况：数组大小；判断rgIdx是否越界；作为循环条件
    @Override
    public long getNumberOfRows()
    {
        throw new UnsupportedOperationException("getNumberOfRows is not supported in a stream");
    }

    /**
     * Get the compression codec used in this file. Currently unused and thus unsupported
     */
    @Override
    public PixelsProto.CompressionKind getCompressionKind()
    {
        throw new UnsupportedOperationException("getCompressionKind is currently not supported");
    }

    /**
     * Get the compression block size. Currently unused and thus unsupported
     */
    @Override
    public long getCompressionBlockSize()
    {
        throw new UnsupportedOperationException("getCompressionBlockSize is currently not supported");
    }

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    @Override
    public long getPixelStride()
    {
        return this.streamHeader.getPixelStride();
    }

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    @Override
    public String getWriterTimeZone()
    {
        return this.streamHeader.getWriterTimezone();
    }

    /**
     * Get schema of this file
     *
     * @return schema
     */
    @Override
    public TypeDescription getFileSchema()
    {
        try
        {
            streamHeaderLatch.await();
        } catch (InterruptedException e)
        {
            logger.error("Interrupted while waiting for stream header", e);
        }
        return this.fileSchema;
    }

    /**
     * Unsupported: In streaming mode, the number of row groups in current stream cannot be determined in advance.
     */
    @Override
    public int getRowGroupNum()
    {
        throw new UnsupportedOperationException("getRowGroupNum is not supported in a stream");
    }

    @Override
    public boolean isPartitioned()
    {
        try
        {
            streamHeaderLatch.await();
        } catch (InterruptedException e)
        {
            logger.error("Interrupted while waiting for stream header", e);
        }
        return this.streamHeader.hasPartitioned() && this.streamHeader.getPartitioned();
    }

    /**
     * Get file level statistics of each column. Not required in streaming mode
     */
    @Override
    public List<PixelsProto.ColumnStatistic> getColumnStats()
    {
        throw new UnsupportedOperationException("getColumnStats is not supported in a stream");
    }

    /**
     * Get file level statistic of the specified column. Currently unused and unsupported
     */
    @Override
    public PixelsProto.ColumnStatistic getColumnStat(String columnName)
    {
        throw new UnsupportedOperationException("getColumnStat is not supported in a stream");
    }

    /**
     * Get information of all row groups. Currently unused and unsupported
     */
    @Override
    public List<PixelsProto.RowGroupInformation> getRowGroupInfos()
    {
        throw new UnsupportedOperationException("getRowGroupInfos is not supported in a stream");
    }

    /**
     * Get information of specified row group. Currently unused and unsupported
     */
    @Override
    public PixelsProto.RowGroupInformation getRowGroupInfo(int rowGroupId)
    {
        throw new UnsupportedOperationException("getRowGroupInfo is not supported in a stream");
    }

    /**
     * Get statistics of the specified row group. Currently unused and unsupported
     */
    @Override
    public PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId)
    {
        throw new UnsupportedOperationException("getRowGroupStat is not supported in a stream");
    }

    /**
     * Get statistics of all row groups. Currently unused and unsupported
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats()
    {
        throw new UnsupportedOperationException("getRowGroupStats is not supported in a stream");
    }

    @Override
    public PixelsProto.PostScript getPostScript()
    {
        throw new UnsupportedOperationException("getPostScript is not supported in a stream");
    }

    @Override
    public PixelsProto.Footer getFooter()
    {
        throw new UnsupportedOperationException("getFooter is not supported in a stream");
    }

    /**
     * Cleanup and release resources
     *
     * @throws IOException
     */
    @Override
    public void close()
            throws IOException
    {
        // new Thread().start(): A low-level approach to create and start a new thread.
        // Use new Thread().start() for simple, one-off asynchronous tasks where the overhead of managing
        // a thread pool is unnecessary.
        new Thread(() -> {
            try
            {
                if (!this.httpServerFuture.isDone()) this.httpServerFuture.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException e)
            {
                logger.warn("In close(), HTTP server did not shut down in 5 seconds, doing forceful shutdown");
                this.httpServerFuture.cancel(true);
            } catch (InterruptedException | ExecutionException e)
            {
                logger.error("Exception during HTTP server shutdown", e);
            } finally
            {
                for (PixelsRecordReader recordReader : recordReaders)
                {
                    try
                    {
                        recordReader.close();
                    } catch (IOException e)
                    {
                        logger.error("Exception while closing record reader", e);
                    }
                }
            }
        }).start();
    }
}
