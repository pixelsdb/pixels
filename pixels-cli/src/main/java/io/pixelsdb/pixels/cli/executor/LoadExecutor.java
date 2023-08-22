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
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.pixelsdb.pixels.cli.Main.validateOrderOrCompactPath;

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
        String origin = ns.getString("original_data_path");
        int rowNum = Integer.parseInt(ns.getString("row_num"));
        String regex = ns.getString("row_regex");
        String paths = ns.getString("loading_data_paths");
        int threadNum = Integer.parseInt(ns.getString("consumer_thread_num"));
        EncodingLevel encodingLevel = EncodingLevel.from(Integer.parseInt(ns.getString("encoding_level")));
        System.out.println("encoding level: " + encodingLevel);
        boolean nullsPadding = Boolean.parseBoolean(ns.getString("nulls_padding"));
        String[] loadingDataPaths = null;
        if (paths != null)
        {
            loadingDataPaths = paths.split(";");
            if (loadingDataPaths.length > 0)
            {
                validateOrderOrCompactPath(loadingDataPaths);
            }
        }

        if (!origin.endsWith("/"))
        {
            origin += "/";
        }

        Storage storage = StorageFactory.Instance().getStorage(origin);

        Parameters parameters = new Parameters(schemaName, tableName, rowNum, regex,
                encodingLevel, nullsPadding, loadingDataPaths);

        // source already exist, producer option is false, add list of source to the queue
        List<String> fileList = storage.listPaths(origin);
        BlockingQueue<String> fileQueue = new LinkedBlockingQueue<>(fileList.size());
        for (String filePath : fileList)
        {
            fileQueue.add(storage.ensureSchemePrefix(filePath));
        }

        long startTime = System.currentTimeMillis();
        if (startConsumer(threadNum, fileQueue, parameters))
        {
            System.out.println("Execute command " + command + " successful");
        } else
        {
            System.out.println("Execute command " + command + " failed");
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Text files in '" + origin + "' are loaded by " + threadNum +
                " threads in " + (endTime - startTime) / 1000 + "s.");
    }

    private boolean startConsumer(int threadNum, BlockingQueue<String> queue, Parameters parameters)
    {
        ConfigFactory configFactory = ConfigFactory.Instance();

        boolean success = false;
        try
        {
            // initialize the extra parameters for data loading
            success = parameters.initExtra(configFactory);
        } catch (MetadataException | InterruptedException e)
        {
            e.printStackTrace();
        }

        boolean res = false;
        if (success)
        {
            Consumer[] consumers = new Consumer[threadNum];
            try
            {
                for (int i = 0; i < threadNum; i++)
                {
                    PixelsConsumer pixelsConsumer = new PixelsConsumer(queue, parameters, i);
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
            System.out.println("Parameters initialization error.");
        }
        return res;
    }
}
