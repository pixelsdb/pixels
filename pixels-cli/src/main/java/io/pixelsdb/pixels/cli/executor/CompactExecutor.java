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

import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.compactor.CompactLayout;
import io.pixelsdb.pixels.core.compactor.PixelsCompactor;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.pixelsdb.pixels.cli.Main.validateOrderOrCompactPath;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-04-16
 */
public class CompactExecutor implements CommandExecutor
{
    @Override
    public void execute(Namespace ns, String command) throws Exception
    {
        String schemaName = ns.getString("schema");
        String tableName = ns.getString("table");
        String naive = ns.getString("naive");
        int threadNum = Integer.parseInt(ns.getString("concurrency"));
        ExecutorService compactExecutor = Executors.newFixedThreadPool(threadNum);

        String metadataHost = ConfigFactory.Instance().getProperty("metadata.server.host");
        int metadataPort = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));

        // get compact layout
        MetadataService metadataService = new MetadataService(metadataHost, metadataPort);
        List<Layout> layouts = metadataService.getLayouts(schemaName, tableName);
        metadataService.shutdown();
        System.out.println("existing number of layouts: " + layouts.size());
        Layout layout = null;
        for (Layout layout1 : layouts)
        {
            if (layout1.isWritable())
            {
                layout = layout1;
                break;
            }
        }

        requireNonNull(layout, String.format("writable layout is not found for table '%s.%s'.",
                schemaName, tableName));
        Compact compact = layout.getCompact();
        int numRowGroupInBlock = compact.getNumRowGroupInFile();
        int numColumn = compact.getNumColumn();
        CompactLayout compactLayout;
        if (naive.equalsIgnoreCase("yes") || naive.equalsIgnoreCase("y"))
        {
            compactLayout = CompactLayout.buildNaive(numRowGroupInBlock, numColumn);
        }
        else
        {
            compactLayout = CompactLayout.fromCompact(compact);
        }

        // get input file paths
        ConfigFactory configFactory = ConfigFactory.Instance();
        validateOrderOrCompactPath(layout.getOrderedPathUris());
        validateOrderOrCompactPath(layout.getCompactPathUris());
        // PIXELS-399: it is not a problem if the order or compact path contains multiple directories
        Storage orderStorage = StorageFactory.Instance().getStorage(layout.getOrderedPathUris()[0]);
        Storage compactStorage = StorageFactory.Instance().getStorage(layout.getCompactPathUris()[0]);
        long blockSize = Long.parseLong(configFactory.getProperty("block.size"));
        short replication = Short.parseShort(configFactory.getProperty("block.replication"));
        List<Status> statuses = orderStorage.listStatus(layout.getOrderedPathUris());
        String[] targetPaths = layout.getCompactPathUris();
        int targetPathId = 0;

        // compact
        long startTime = System.currentTimeMillis();
        for (int i = 0, thdId = 0; i < statuses.size(); i += numRowGroupInBlock, ++thdId)
        {
            if (i + numRowGroupInBlock > statuses.size())
            {
                /**
                 * Issue #160:
                 * Compact the tail files that can not fulfill the compactLayout
                 * defined in the metadata.
                 * Note that if (i + numRowGroupInBlock == statues.size()),
                 * then the remaining files are not tail files.
                 *
                 * Here we set numRowGroupInBlock to the number of tail files,
                 * and rebuild a pure compactLayout for the tail files as the
                 * compactLayout in metadata does not work for the tail files.
                 */
                numRowGroupInBlock = statuses.size() - i;
                compactLayout = CompactLayout.buildPure(numRowGroupInBlock, numColumn);
            }

            List<String> sourcePaths = new ArrayList<>();
            for (int j = 0; j < numRowGroupInBlock; ++j)
            {
                if (!statuses.get(i+j).getPath().endsWith("/"))
                {
                    sourcePaths.add(statuses.get(i + j).getPath());
                }
            }

            String targetDirPath = targetPaths[targetPathId++];
            targetPathId %= targetPaths.length;
            if (!targetDirPath.endsWith("/"))
            {
                targetDirPath += "/";
            }
            String targetFilePath = targetDirPath + DateUtil.getCurTime() + "_compact.pxl";

            System.out.println("(" + thdId + ") " + sourcePaths.size() +
                    " ordered files to be compacted into '" + targetFilePath + "'.");

            PixelsCompactor.Builder compactorBuilder =
                    PixelsCompactor.newBuilder()
                            .setSourcePaths(sourcePaths)
                            /**
                             * Issue #192:
                             * No need to deep copy compactLayout as it is never modified in-place
                             * (e.g., call setters to change some members). Thus it is safe to use
                             * the current reference of compactLayout even if the compactors will
                             * be running multiple threads.
                             *
                             * Deep copy it if it is in-place modified in the future.
                             */
                            .setCompactLayout(compactLayout)
                            .setInputStorage(orderStorage)
                            .setOutputStorage(compactStorage)
                            .setPath(targetFilePath)
                            .setBlockSize(blockSize)
                            .setReplication(replication)
                            .setBlockPadding(false);

            long threadStart = System.currentTimeMillis();
            compactExecutor.execute(() -> {
                // Issue #192: run compaction in threads.
                try
                {
                    // build() spends some time to read file footers and should be called inside sub-thread.
                    PixelsCompactor pixelsCompactor = compactorBuilder.build();
                    pixelsCompactor.compact();
                    pixelsCompactor.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
                System.out.println("Compact file '" + targetFilePath + "' is built in " +
                        ((System.currentTimeMillis() - threadStart) / 1000.0) + "s");
            });
        }

        // Issue #192: wait for the compaction to complete.
        compactExecutor.shutdown();
        while (!compactExecutor.awaitTermination(100, TimeUnit.SECONDS));

        long endTime = System.currentTimeMillis();
        System.out.println("Pixels files in '" + Joiner.on(";").join(layout.getOrderedPathUris()) + "' are compacted into '" +
                Joiner.on(";").join(layout.getCompactPathUris()) + "' by " + threadNum + " threads in " +
                (endTime - startTime) / 1000 + "s.");
    }
}
