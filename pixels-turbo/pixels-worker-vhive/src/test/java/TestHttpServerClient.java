import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.worker.common.WorkerException;
import io.pixelsdb.pixels.worker.vhive.PixelsReaderStreamImpl;
import io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestHttpServerClient {

    @Test
    public void testServerSimple() throws Exception {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(false);
        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
//        String[] colNames = new String[]{"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
        option.includeCols(colNames);
        // option.transId(1);

        String serverIpAddress = "127.0.0.1";
        int serverPort = 8080;
        PixelsReaderStreamImpl reader = new PixelsReaderStreamImpl("http://" + serverIpAddress + ":" + serverPort + "/");
        PixelsRecordReader recordReader = reader.read(option);
        // use an array of readers, to support multiple streams (relies on
        //  a service framework to map endpoints to IDs. todo)
        while (true) {
            try {
                VectorizedRowBatch rowBatch = recordReader.readBatch(5); // (7);
                if (rowBatch.size == 0) {reader.close(); break;}
                System.out.println("Parsed rowBatch: ");
                System.out.println(rowBatch);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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

    @Test
    public void testClientSimple() throws IOException {
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
        // option.transId(1);
        PixelsRecordReader recordReader = reader.build().read(option);

        String serverIpAddress = "127.0.0.1";
        int serverPort = 8080;
        PixelsWriter pixelsWriter = PixelsWriterStreamImpl.newBuilder()
                .setUri(URI.create("http://" + serverIpAddress + ":" + serverPort + "/"))
                .setSchema(recordReader.getResultSchema())
                .setPixelStride(10000)
                .setRowGroupSize(1048576)  // send a packet per 1MB (segmentation possible)
                // .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setEncodingLevel(EncodingLevel.EL2) // it is worth to do encoding
                .setPartitioned(false)
                .build();
        // XXX: now we can send multiple rowBatches in one rowGroup in one packet, but have not tested to send multiple rowGroups (not necessary, though)
        while (true) {
            VectorizedRowBatch rowBatch = recordReader.readBatch(5);
            System.out.println(rowBatch.size + " rows read from tpch nation.pxl");

            try {
                if (rowBatch.size == 0) {pixelsWriter.close(); break;}
                else pixelsWriter.addRowBatch(rowBatch);
            } catch (Throwable e) {
                throw new WorkerException("failed to write rowBatch to HTTP server", e);
            }
        }

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
