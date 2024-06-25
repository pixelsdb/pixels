/*
 * Copyright 2024 PixelsDB.
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

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2024-06-25
 */
public class ImportExecutor implements CommandExecutor
{
    @Override
    public void execute(Namespace ns, String command) throws Exception
    {
        String schemaName = ns.getString("schema");
        String tableName = ns.getString("table");
        String layoutName = ns.getString("layout");
        checkArgument(layoutName.equalsIgnoreCase("ordered") ||
                layoutName.equalsIgnoreCase("compact"),
                "layout must be 'ordered' or 'compact'");
        boolean ordered = layoutName.equalsIgnoreCase("ordered");
        String path = ns.getString("path");

        if (!path.endsWith("/"))
        {
            path += "/";
        }

        Storage storage = StorageFactory.Instance().getStorage(path);
        List<String> filePaths = storage.listPaths(path);

        String metadataHost = ConfigFactory.Instance().getProperty("metadata.server.host");
        int metadataPort = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));
        MetadataService metadataService = new MetadataService(metadataHost, metadataPort);
        Layout writableLayout = metadataService.getWritableLayout(schemaName, tableName);
        if (writableLayout == null)
        {
            System.err.println("no writable layout on table '" + schemaName + "." + tableName + "' for data import");
            return;
        }

        List<File> importFiles = getImportFiles(ordered, writableLayout, filePaths);
        metadataService.addFiles(importFiles);
        metadataService.shutdown();
    }

    private static List<File> getImportFiles(boolean ordered, Layout writableLayout, List<String> filePaths)
    {
        List<Path> dirPaths = ordered ? writableLayout.getOrderedPaths() : writableLayout.getCompactPaths();
        List<File> importFiles = new LinkedList<>();
        int pathId = 0;
        for (String filePath : filePaths)
        {
            Path dirPath = dirPaths.get(pathId++ % dirPaths.size());
            File importFile = new File();
            importFile.setName(filePath.substring(filePath.lastIndexOf("/")));
            // TODO: set num row group.
            importFile.setPathId(dirPath.getId());
            importFiles.add(importFile);
        }
        return importFiles;
    }
}
