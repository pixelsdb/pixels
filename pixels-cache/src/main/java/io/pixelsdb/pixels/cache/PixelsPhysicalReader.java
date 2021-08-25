/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import io.pixelsdb.pixels.common.exception.FSException;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsProto;

import java.io.IOException;

/**
 * @author guodong
 */
public class PixelsPhysicalReader
{
    private final PhysicalReader physicalReader;
    private final PixelsProto.FileTail fileTail;

    public PixelsPhysicalReader(Storage storage, String path)
    {
        this.physicalReader = PhysicalReaderUtil.newPhysicalReader(storage, path);
        this.fileTail = readFileTail();
    }

    private PixelsProto.FileTail readFileTail()
    {
        if (physicalReader != null)
        {
            try
            {
                long fileLen = physicalReader.getFileLength();
                physicalReader.seek(fileLen - Long.BYTES);
                long fileTailOffset = physicalReader.readLong();
                int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
                physicalReader.seek(fileTailOffset);
                byte[] fileTailBuffer = new byte[fileTailLength];
                physicalReader.readFully(fileTailBuffer);
                return PixelsProto.FileTail.parseFrom(fileTailBuffer);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return null;
    }

    public PixelsProto.RowGroupFooter readRowGroupFooter(int rowGroupId)
            throws IOException
    {
        PixelsProto.RowGroupInformation rgInfo = fileTail.getFooter().getRowGroupInfos(rowGroupId);
        long rgFooterOffset = rgInfo.getFooterOffset();
        int rgFooterLength = rgInfo.getFooterLength();
        byte[] rgFooterBytes = new byte[rgFooterLength];
        physicalReader.seek(rgFooterOffset);
        physicalReader.readFully(rgFooterBytes);

        return PixelsProto.RowGroupFooter.parseFrom(rgFooterBytes);
    }

    public byte[] read(long offset, int length)
            throws IOException
    {
        byte[] content = new byte[length];
        physicalReader.seek(offset);
        physicalReader.readFully(content);

        return content;
    }

    public long getCurrentBlockId() throws FSException, IOException
    {
        return physicalReader.getCurrentBlockId();
    }
}
