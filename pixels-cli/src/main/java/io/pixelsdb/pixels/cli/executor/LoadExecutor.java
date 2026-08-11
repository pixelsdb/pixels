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
package io.pixelsdb.pixels.cli.executor;

import io.pixelsdb.pixels.cli.load.*;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.daemon.NodeProto;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2023-04-16
 */
public class LoadExecutor implements CommandExecutor
{
    private final RetinaService defaultRetinaService = RetinaService.Instance();

    @Override
    public void execute(Namespace ns, String command) throws Exception
    {
        String schemaName = ns.getString("schema");
        String tableName = ns.getString("table");
        String origin = ns.getString("origin");
        int rowNum = Integer.parseInt(ns.getString("row_num"));
        String regex = ns.getString("row_regex");
        int threadNum = Integer.parseInt(ns.getString("consumer_thread_num"));
        EncodingLevel encodingLevel = EncodingLevel.from(Integer.parseInt(ns.getString("encoding_level")));
        System.out.println("encoding level: " + encodingLevel);
        boolean nullsPadding = Boolean.parseBoolean(ns.getString("nulls_padding"));

        if (!origin.endsWith("/"))
        {
            origin += "/";
        }

        TransService transService = TransService.Instance();
        TransContext context = transService.beginTrans(false);
        MetadataService metadataService = MetadataService.Instance();
        ConcurrentLinkedQueue<LoadedInfo> loadedInfos = new ConcurrentLinkedQueue<>();
        long startTime = System.currentTimeMillis();
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(origin);
            Parameters parameters = new Parameters(schemaName, tableName, rowNum, regex,
                    encodingLevel, nullsPadding, metadataService, context.getTransId(), context.getTimestamp());

            // source already exist, producer option is false, add list of source to the queue
            List<String> fileList = storage.listPaths(origin);
            BlockingQueue<String> inputFiles = new LinkedBlockingQueue<>(fileList.size());
            for (String filePath : fileList)
            {
                inputFiles.add(storage.ensureSchemePrefix(filePath));
            }

            startConsumers(threadNum, inputFiles, parameters, loadedInfos);

            int retinaServerPort = Integer.parseInt(ConfigFactory.Instance().getProperty("retina.server.port"));
            for(LoadedInfo loadedInfo : loadedInfos)
            {
                File file = loadedInfo.loadedFile;
                Path path = loadedInfo.loadedPath;
                if (!metadataService.updateFile(file))
                {
                    throw new MetadataException("failed to publish loaded file " + file.getName());
                }
                try
                {

                    NodeProto.NodeInfo nodeInfo = loadedInfo.loadedRetinaNode;
                    if(nodeInfo == null)
                    {
                        if (defaultRetinaService.isEnabled())
                        {
                            defaultRetinaService.addVisibility(File.getFilePath(path, file));
                        }
                    } else
                    {
                        RetinaService retinaService = RetinaService.CreateInstance(nodeInfo.getAddress(), retinaServerPort);
                        if (retinaService.isEnabled())
                        {
                            retinaService.addVisibility(File.getFilePath(path, file));
                        }
                    }

                } catch (RetinaException e)
                {
                    System.out.println("add visibility for ordered file '" + file + "' failed");
                }
            }

            transService.commitTrans(context.getTransId(), false);
            System.out.println(command + " is successful");
        } catch (Exception failure)
        {
            System.err.println(command + " failed");
            List<Long> fileIds = new ArrayList<>();
            for (LoadedInfo loadedInfo : loadedInfos)
            {
                if (loadedInfo.loadedFile != null)
                {
                    fileIds.add(loadedInfo.loadedFile.getId());
                }
            }
            if (!fileIds.isEmpty())
            {
                try
                {
                    if (!metadataService.deleteFiles(fileIds))
                    {
                        failure.addSuppressed(new MetadataException(
                                "failed to delete unpublished load files " + fileIds));
                    }
                } catch (Exception cleanupFailure)
                {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            try
            {
                transService.rollbackTrans(context.getTransId(), false);
            } catch (Exception rollbackFailure)
            {
                failure.addSuppressed(rollbackFailure);
            }
            throw failure;
        } finally
        {
            long endTime = System.currentTimeMillis();
            System.out.println("Text files in '" + origin + "' are loaded by " + threadNum +
                        " threads in " + (endTime - startTime) / 1000.0 + "s.");
        }
    }

    /**
     * Start concurrent consumers that consumes the input (source) files and load them into pixels files of a table.
     * @param concurrency the number of threads for data loading
     * @param inputFiles the queue of the paths of input files
     * @param parameters the parameters for data loading, e.g., the schema name and table name
     * @param loadedInfos the information of the loaded pixels files
     * @throws Exception if parameters cannot be initialized or a consumer fails
     */
    private void startConsumers(int concurrency, BlockingQueue<String> inputFiles, Parameters parameters,
                                ConcurrentLinkedQueue<LoadedInfo> loadedInfos) throws Exception
    {
        if (!parameters.initExtra())
        {
            throw new IllegalStateException("Parameters initialization error.");
        }

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<?>> futures = new ArrayList<>(concurrency);
        try
        {
            for (int i = 0; i < concurrency; i++)
            {
                Consumer consumer;
                if (parameters.getIndex() == null)
                {
                    consumer = new SimplePixelsConsumer(inputFiles, parameters, loadedInfos);
                } else
                {
                    consumer = new IndexedPixelsConsumer(inputFiles, parameters, loadedInfos);
                }
                futures.add(executor.submit(consumer));
            }

            for (Future<?> future : futures)
            {
                future.get();
            }
        } catch (ExecutionException e)
        {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new Exception(cause.getClass().getSimpleName() +
                    (cause.getMessage() == null ? " failed" : " failed: " + cause.getMessage()), cause);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new Exception("Interrupted while waiting for consumers.", e);
        } finally
        {
            executor.shutdownNow();
            if (!executor.awaitTermination(60, TimeUnit.SECONDS))
            {
                System.err.println("Timed out waiting for consumer threads to terminate.");
            }
        }
    }
}
