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
import com.google.protobuf.util.JsonFormat;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Print the file metadata to screen.
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
            System.out.println("FileFooter:");
            PixelsProto.Footer footer = pixelsReader.getFooter();
            System.out.println(JsonFormat.printer().print(footer));
            System.out.println();
            System.out.println("PostScript:");
            PixelsProto.PostScript postScript = pixelsReader.getPostScript();
            System.out.println(JsonFormat.printer().print(postScript));
            System.out.println();
            System.out.println("File path: " + filePath);
        } catch (Exception e)
        {
            System.err.println("print metadata of file '" + filePath + "' failed");
            e.printStackTrace();
        }
    }
}
