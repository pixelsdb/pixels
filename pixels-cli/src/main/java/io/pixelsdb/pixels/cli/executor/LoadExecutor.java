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

import io.pixelsdb.pixels.cli.load.Consumer;
import io.pixelsdb.pixels.cli.load.Parameters;
import io.pixelsdb.pixels.cli.load.PixelsConsumer;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author hank
 * @create 2023-04-16
 */
public class LoadExecutor implements CommandExecutor
{
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

        TransService transService = new TransService(
                ConfigFactory.Instance().getProperty("trans.server.host"),
                Integer.parseInt(ConfigFactory.Instance().getProperty("trans.server.port")));

        TransContext context;
        context = transService.beginTrans(false);

        Storage storage = StorageFactory.Instance().getStorage(origin);
        MetadataService metadataService = MetadataService.Instance();

        Parameters parameters = new Parameters(schemaName, tableName, rowNum, regex,
                encodingLevel, nullsPadding, metadataService, context.getTransId(), context.getTimestamp());

        // source already exist, producer option is false, add list of source to the queue
        List<String> fileList = storage.listPaths(origin);
        BlockingQueue<String> inputFiles = new LinkedBlockingQueue<>(fileList.size());
        ConcurrentLinkedQueue<File> loadedFiles = new ConcurrentLinkedQueue<>();
        for (String filePath : fileList)
        {
            inputFiles.add(storage.ensureSchemePrefix(filePath));
        }

        long startTime = System.currentTimeMillis();
        if (startConsumers(threadNum, inputFiles, parameters, loadedFiles))
        {
            metadataService.addFiles(loadedFiles);
            System.out.println(command + " is successful");
        } else
        {
            System.err.println(command + " failed");
        }

        transService.commitTrans(context.getTransId(), context.getTimestamp());

        long endTime = System.currentTimeMillis();
        System.out.println("Text files in '" + origin + "' are loaded by " + threadNum +
                " threads in " + (endTime - startTime) / 1000.0 + "s.");
    }

    /**
     * Start concurrent consumers that consumes the input (source) files and load them into pixels files of a table.
     * @param concurrency the number of threads for data loading
     * @param inputFiles the queue of the paths of input files
     * @param parameters the parameters for data loading, e.g., the schema name and table name
     * @param loadedFiles the information of the loaded pixels files
     * @return true if consumers complete successfully
     */
    private boolean startConsumers(int concurrency, BlockingQueue<String> inputFiles, Parameters parameters,
                                   ConcurrentLinkedQueue<File> loadedFiles)
    {
        boolean success = false;
        try
        {
            // initialize the extra parameters for data loading
            success = parameters.initExtra();
        } catch (MetadataException | InterruptedException e)
        {
            e.printStackTrace();
        }

        boolean res = false;
        if (success)
        {
            Consumer[] consumers = new Consumer[concurrency];
            try
            {
                for (int i = 0; i < concurrency; i++)
                {
                    PixelsConsumer pixelsConsumer = new PixelsConsumer(inputFiles, parameters, loadedFiles);
                    consumers[i] = pixelsConsumer;
                    pixelsConsumer.start();
                }
                for (Consumer c : consumers)
                {
                    try
                    {
                        c.join();
                    } catch (InterruptedException e)
                    {
                        throw new Exception("Consumer InterruptedException, " + e.getMessage());
                    }
                }
                res = true;
            } catch (Exception e)
            {
                try
                {
                    throw new Exception("Consumer Error, " + e.getMessage());
                } catch (Exception e1)
                {
                    e1.printStackTrace();
                }
            }
        } else
        {
            System.err.println("Parameters initialization error.");
        }
        return res;
    }
}
