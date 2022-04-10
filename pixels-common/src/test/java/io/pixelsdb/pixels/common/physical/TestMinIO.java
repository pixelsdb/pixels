/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.common.physical;

import org.junit.Test;

import java.io.IOException;

import static io.pixelsdb.pixels.common.utils.Constants.*;

/**
 * @author hank
 * Created at: 10/04/2022
 */
public class TestMinIO
{
    @Test
    public void testRead() throws IOException
    {
        System.getProperties().setProperty(SYS_MINIO_ENDPOINT, "");
        System.getProperties().setProperty(SYS_MINIO_ACCESS_KEY, "");
        System.getProperties().setProperty(SYS_MINIO_SECRET_KEY, "");
        Storage minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
        minio.listPaths("");
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(minio, "");
        reader.supportsAsync();
        reader.readFully(10);
    }
}
