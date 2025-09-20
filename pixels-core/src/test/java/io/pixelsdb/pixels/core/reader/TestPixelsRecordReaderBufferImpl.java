/*
 * Copyright 2025 PixelsDB.
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

package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.junit.Test;

import java.io.IOException;

public class TestPixelsRecordReaderBufferImpl
{

    @Test
    public void prepareRetinaBuffer()
    {

    }

    @Test
    public void testReadBatch() throws RetinaException, IOException
    {

        Storage storage = StorageFactory.Instance().getStorage("minio");
        String schemaName = "pixels_tpch";
        String tableName = "test";


        RetinaService retinaService = RetinaService.Instance();
        long timeStamp = 100000;
        RetinaProto.GetWriterBufferResponse superVersion = retinaService.getWriterBuffer(schemaName, tableName, timeStamp);
        TypeDescription typeDescription = null;


        PixelsReaderOption option = new PixelsReaderOption();
        option.transId(100)
                .transTimestamp(timeStamp);

        com.google.protobuf.ByteString byteString = superVersion.getData();
    }
}
