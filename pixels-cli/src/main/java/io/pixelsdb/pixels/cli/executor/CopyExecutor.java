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

import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.DateUtil;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.io.IOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hank
 * @create 2023-04-16
 */
public class CopyExecutor implements CommandExecutor
{
    @Override
    public void execute(Namespace ns, String command) throws Exception
    {
        String postfix = ns.getString("postfix");
        String source = ns.getString("source");
        String destination = ns.getString("destination");
        int n = Integer.parseInt(ns.getString("number"));
        int threadNum = Integer.parseInt(ns.getString("concurrency"));
        ExecutorService copyExecutor = Executors.newFixedThreadPool(threadNum);

        if (!destination.endsWith("/"))
        {
            destination += "/";
        }

        ConfigFactory configFactory = ConfigFactory.Instance();

        Storage sourceStorage = StorageFactory.Instance().getStorage(source);
        Storage destStorage = StorageFactory.Instance().getStorage(destination);

        List<Status> files =  sourceStorage.listStatus(source);
        long blockSize = Long.parseLong(configFactory.getProperty("block.size"));
        short replication = Short.parseShort(configFactory.getProperty("block.replication"));

        // copy
        long startTime = System.currentTimeMillis();
        AtomicInteger copiedNum = new AtomicInteger(0);
        for (int i = 0; i < n; ++i)
        {
            String destination_ = destination;
            // Issue #192: make copy multithreaded.
            for (Status s : files)
            {
                String sourceName = s.getName();
                if (!sourceName.contains(postfix))
                {
                    continue;
                }
                String destPath = destination_ +
                        sourceName.substring(0, sourceName.indexOf(postfix)) +
                        "_copy_" + DateUtil.getCurTime() + postfix;
                copyExecutor.execute(() -> {
                    try
                    {
                        if (sourceStorage.getScheme() == destStorage.getScheme() &&
                                destStorage.supportDirectCopy())
                        {
                            destStorage.directCopy(s.getPath(), destPath);
                        } else
                        {
                            DataInputStream inputStream = sourceStorage.open(s.getPath());
                            DataOutputStream outputStream = destStorage.create(destPath, false,
                                    Constants.HDFS_BUFFER_SIZE, replication, blockSize);
                            IOUtils.copyBytes(inputStream, outputStream,
                                    Constants.HDFS_BUFFER_SIZE, true);
                        }
                        copiedNum.incrementAndGet();
                    } catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                });
            }
        }

        copyExecutor.shutdown();
        while (!copyExecutor.awaitTermination(100, TimeUnit.SECONDS));

        long endTime = System.currentTimeMillis();
        System.out.println((copiedNum.get()/n) + " file(s) are copied " + n + " time(s) by "
                + threadNum + " threads in " + (endTime - startTime) / 1000 + "s.");
    }
}
