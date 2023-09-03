import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.HttpServer;
import io.pixelsdb.pixels.common.utils.HttpServerHandler;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DictionaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.worker.common.WorkerException;
import io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl;
import io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.request.body.multipart.Part;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.pixelsdb.pixels.common.utils.Constants.MAGIC;
// import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;

public class TestHttpServerClient {

    @Test
    public void testServerSimple() throws Exception {

        HttpServer h;
        h = new HttpServer(new HttpServerHandler() {
            @Override
            public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (!(msg instanceof HttpRequest)) return;
                FullHttpRequest req = (FullHttpRequest) msg;

                // if (req.method() != HttpMethod.POST) {sendHttpResponse(ctx, HttpResponseStatus.OK);}
                ByteBuf content = req.content();
                byte[] bytes = new byte[content.readableBytes()];
                content.readBytes(bytes);
                System.out.println("HTTP request object body total length: " + bytes.length);

                if (!Objects.equals(req.headers().get("Content-Type"), "application/x-protobuf")) { return; }
                int magicLength = MAGIC.getBytes().length;
                String magic = new String(bytes, 0, magicLength);
                System.out.println("Parsed magic: " + magic);

                int metadataLength = content.getInt(magicLength);  // getInt(int index)
                System.out.println("Parsed metadataLength: " + metadataLength);

                PixelsProto.PipeliningMetadata metadata = null;
                try {
                    metadata = PixelsProto.PipeliningMetadata.parseFrom(
                        Arrays.copyOfRange(bytes,
                            magicLength + Integer.BYTES,
                            magicLength + Integer.BYTES + metadataLength));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                int messageBodyOffset = calculateCeiling(magicLength + Integer.BYTES + metadataLength, 8);
                    // todo consume the padding bytes
                    System.out.println("Parsed messageBodyOffset: " + messageBodyOffset);
                    System.out.println("Parsed metadata object: " + metadata);

                    final int rowCount = metadata.getNumberOfRows();
                    ByteBuf messageBodyByteBuf = Unpooled.wrappedBuffer(bytes, messageBodyOffset, metadata.getBodyLength());

                    PixelsReaderStreamImpl.Builder reader = PixelsReaderStreamImpl.newBuilder()
                            .setBuilderBufReader(messageBodyByteBuf)
                            .setBuilderTotalBufLen(metadata.getBodyLength());
                    PixelsReaderOption option = new PixelsReaderOption();
                    option.skipCorruptRecords(true);
                    option.tolerantSchemaEvolution(true);
                    option.enableEncodedColumnVector(false);
                    String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
//            String[] colNames = new String[]{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
                    option.includeCols(colNames);
                    // option.rgRange(0, 1);
                    // option.transId(1);
                PixelsRecordReader recordReader = null;
                try {
                    recordReader = reader.build().read(option);
                    VectorizedRowBatch rowBatch = recordReader.readBatch(1000);
                    System.out.println("Parsed rowBatch: ");
                    System.out.println(rowBatch);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK);
                ChannelFuture f = ctx.writeAndFlush(response);
                    f.addListener(future -> {
                        if (!future.isSuccess()) {
                            System.out.println("Failed to write response: " + future.cause());
                            // ctx.close(); // Close the channel on error
                        }
                    });
                    f.addListener(ChannelFutureListener.CLOSE);
                            // .addListener(future -> {
                            //     // Gracefully shutdown the server after the channel is closed
                            //     ctx.channel().parent().close().addListener(ChannelFutureListener.CLOSE);
                            // }); // serve only once, so that we pass the test instead of hanging
            }
        });
        h.serve();
    }

    void runAsync(Runnable fp, int concurrency) {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        CompletableFuture<Void>[] httpServerFutures = new CompletableFuture[concurrency];
        for (int i = 0; i < concurrency; i++) {
            httpServerFutures[i] = CompletableFuture.runAsync(() -> {
                try {
                    fp.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, executorService);
            if (i < concurrency - 1) {
                System.out.println("Booted " + (i + 1) + " http clients");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            for (int i = 10; i > 0; i--) {
                System.out.printf("Main thread is still running... %d\n", i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        // Perform your other calculations asynchronously
//        CompletableFuture<Void> calculationsFuture = CompletableFuture.runAsync(() -> {
//            // Your calculations here
//            System.out.println("Performing calculations...");
//        }, executorService);

        // Wait for both futures to complete
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(httpServerFutures); // , calculationsFuture);
        // Block until both futures are completed
        combinedFuture.join();

        // Shutdown the executor service
        executorService.shutdown();
    }

    @Test
    public void testServerAsync() {
        runAsync(() -> {
            try {
                testServerSimple();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1);
    }

    static int calculateCeiling(int value, int multiple) {
        // to calculate padding length in HttpClient

        if (value <= 0 || multiple <= 0) {
            throw new IllegalArgumentException("Both value and multiple must be positive.");
        }

        int remainder = value % multiple;
        if (remainder == 0) {
            // No need to adjust, value is already a multiple of multiple
            return value;
        }

        int difference = multiple - remainder;
        return value + difference;
    }

    @Test
    public void testClientSimple() throws IOException {
        final ByteBuf buffer = Unpooled.buffer(); // ??? Unpooled.directBuffer();
//        System.out.println(buffer.capacity());

//        ConfigMinio("dummy-region", "http://hp000.utah.cloudlab.us:9000", "", "");
//        Storage minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
//        System.out.println(minio.listPaths("pixels-tpch/").size() + " .pxl files on Minio");
//        List<String> files = minio.listPaths("pixels-tpch/customer/");

        Storage fileStorage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReaderImpl.Builder reader = PixelsReaderImpl.newBuilder()
                .setStorage(fileStorage)
                .setPath("/home/jasha/pixels-tpch/nation/v-0-ordered/20230814143629_105.pxl")
//                .setPath("/home/jasha/pixels-tpch/customer/v-0-ordered/20230814141738_0.pxl")
                .setEnableCache(false)
                .setPixelsFooterCache(new PixelsFooterCache());
//        for (String file : files)
//        {
//            System.out.println(file);
//        }
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(false);
        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
//        String[] colNames = new String[]{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
        option.includeCols(colNames);
        // option.rgRange(0, 1);
        // option.transId(1);
        PixelsRecordReader recordReader = reader.build().read(option);
        VectorizedRowBatch rowBatch = recordReader.readBatch(1000);
        System.out.println(rowBatch.size + " rows read from tpch nation.pxl");

        PixelsProto.PipeliningMetadata.Builder message = PixelsProto.PipeliningMetadata.newBuilder();
        PixelsProto.Type.Builder typeBuild = PixelsProto.Type.newBuilder();
        // 似乎把Pixels文件读到内存里的时候丢弃了column names (PixelsRecordReaderImpl.java: 215)。
        // 所以这里只能依靠前面的option.includeCols(new String[]{"n_nationkey", "n_name", "n_regionkey"});
        // 作为列名。
        for (int i = 0; i < rowBatch.cols.length; i++) {
            ColumnVector col = rowBatch.cols[i];
            typeBuild.setName(colNames[i]);
            if (col instanceof LongColumnVector) {
                typeBuild.setKind(PixelsProto.Type.Kind.LONG).build();
            } else if (col instanceof DictionaryColumnVector) {
                typeBuild.setKind(PixelsProto.Type.Kind.STRING).build();
                // 我们没有StringColumnVector，所以只能用DictionaryColumnVector代替？
            } else {
                typeBuild.setKind(PixelsProto.Type.Kind.INT).build();
            }
            message.addTypes(typeBuild.build());
        }

        buffer.clear();
        PixelsWriter pixelsWriter = PixelsWriterStreamImpl.newBuilder()
                .setSchema(recordReader.getResultSchema())
                .setPixelStride(10000)
                .setRowGroupSize(1048576)  // send a packet per 1MB (segmentation possible)
                // .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setBufWriter(buffer)
                .setEncodingLevel(EncodingLevel.EL2) // it is worth to do encoding
                .setPartitioned(false)
                .build();
        try {
            pixelsWriter.addRowBatch(rowBatch);
            pixelsWriter.close();
        } catch (Throwable e)
        {
            throw new WorkerException("failed to scan the file and output the result", e);
        }

        byte[] magicBytes = MAGIC.getBytes();
        byte[] messageBytes = message.setBodyLength(buffer.readableBytes())
                .setVersion(0)
                .setNumberOfRows(rowBatch.size)
                .build().toByteArray();
        int messageLength = messageBytes.length;
        byte[] messageLengthBytes = ByteBuffer.allocate(Integer.BYTES).putInt(messageLength).array();
        int paddingLength = (8 - (magicBytes.length + messageLengthBytes.length + messageLength) % 8) % 8;  // Can use '&7'
        byte[] paddingBytes = new byte[paddingLength];
        ByteBuf fullBody = Unpooled.wrappedBuffer(2,
                Unpooled.wrappedBuffer(magicBytes, messageLengthBytes, messageBytes, paddingBytes),
                buffer);

        try (AsyncHttpClient httpClient = Dsl.asyncHttpClient()) {
            String serverIpAddress = "127.0.0.1";
            int serverPort = 8080;
            Request req = httpClient.preparePost("http://" + serverIpAddress + ":" + serverPort + "/")
                    .addFormParam("param1", "value1")
                    .setBody(fullBody.nioBuffer())
                    .addHeader(CONTENT_TYPE, "application/x-protobuf")
                    .addHeader(CONTENT_LENGTH, fullBody.readableBytes())
                    .addHeader(CONNECTION, CLOSE)
                    .build();
            // cannot use Unpooled.wrappedBuffer(magicBytes, messageLengthBytes, messageBytes, paddingBytes, buffer.array())
            //  because buffer.array() adds unsolicited padding bytes to the end of the actual buffer. (gets an array of 1024 bytes on my machine)

            Response response = httpClient.executeRequest(req).get();
            System.out.println("HTTP response status code: " + response.getStatusCode());

        } catch (Exception e) {
            e.printStackTrace();
        }

//        buffer.retain();  // ???
//                    try {
//                        for (int i = 10; i > 0; i--) {
//                            System.out.printf("Handler thread is still running... %d\n", i);
//                            Thread.sleep(1000);
//                        }
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
    }

    @Test
    public void testClientAsync() {
        runAsync(() -> {
            try {
                testClientSimple();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1);
    }

    @Test
    public void testClientConcurrent() {
        runAsync(() -> {
            try {
                testClientSimple();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 3);
    }
}
