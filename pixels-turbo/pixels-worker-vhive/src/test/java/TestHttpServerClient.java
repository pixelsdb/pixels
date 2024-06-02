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

/**
 * @author jasha64
 * @create 2023-08-07
 */
public class TestHttpServerClient {

    @Test
    public void testServerSimple() throws Exception {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(false);
        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
        option.includeCols(colNames);

        String serverIpAddress = "127.0.0.1";
        int serverPort = 50100;
        PixelsReaderStreamImpl reader = new PixelsReaderStreamImpl("http://" + serverIpAddress + ":" + serverPort + "/");
        PixelsRecordReader recordReader = reader.read(option);
        // use an array of readers, to support multiple streams (relies on
        //  a service framework to map endpoints to IDs. todo)
        while (true) {
            try {
                VectorizedRowBatch rowBatch = recordReader.readBatch(5);
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

        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(httpServerFutures);
        combinedFuture.join();

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
        Storage fileStorage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReaderImpl.Builder reader = PixelsReaderImpl.newBuilder()
                .setStorage(fileStorage)
                .setPath("/home/jasha/pixels-tpch/nation/v-0-ordered/20230814143629_105.pxl")
                .setEnableCache(false)
                .setPixelsFooterCache(new PixelsFooterCache());
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(false);
        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
        option.includeCols(colNames);
        PixelsRecordReader recordReader = reader.build().read(option);

        String serverIpAddress = "127.0.0.1";
        int serverPort = 50100;
        PixelsWriter pixelsWriter = PixelsWriterStreamImpl.newBuilder()
                .setUri(URI.create("http://" + serverIpAddress + ":" + serverPort + "/"))
                .setSchema(recordReader.getResultSchema())
                .setPixelStride(10000)
                .setRowGroupSize(1048576)  // send a packet per 1MB (segmentation possible)
                // .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setEncodingLevel(EncodingLevel.EL2)
                .setPartitioned(false)
                .build();
        // XXX: now we can send multiple rowBatches in one rowGroup in one packet, but have not tested to send multiple rowGroups
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
