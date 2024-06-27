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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * @author hank
 * @create 2024-06-25
 */
public class FileMetaExecutor implements CommandExecutor
{
    @Override
    public void execute(Namespace ns, String command) throws Exception
    {
        String filePath = ns.getString("file");
        Storage storage = StorageFactory.Instance().getStorage(filePath);
        try (PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath(filePath).setStorage(storage).setEnableCache(false)
                .setCacheOrder(ImmutableList.of()).setPixelsCacheReader(null)
                .setPixelsFooterCache(new PixelsFooterCache()).build())
        {
            int fileVersion = pixelsReader.getPostScript().getVersion();
            if (fileVersion != Constants.FILE_VERSION)
            {
                System.err.println("WARN: file version (" + fileVersion + ") is inconsistent with the version (" +
                        Constants.FILE_VERSION + ") of this PixelsReader, the following output might be incorrect!");
            }
            // TODO: print file metadata
        } catch (Exception e)
        {
            System.err.println("show metadata of file '" + filePath + "' failed");
            e.printStackTrace();
        }
    }
}
