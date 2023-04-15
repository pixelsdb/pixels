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
package io.pixelsdb.pixels.storage.hdfs;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderOption;
import io.pixelsdb.pixels.common.physical.PhysicalReaderProvider;
import io.pixelsdb.pixels.common.physical.Storage;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-04-15
 */
public class HDFSReaderProvider implements PhysicalReaderProvider
{
    @Override
    public PhysicalReader createReader(Storage storage, String path, @Nullable PhysicalReaderOption option)
            throws IOException
    {
        checkArgument(storage.getScheme().equals(Storage.Scheme.hdfs),
                "storage is incompatible with hdfs");
        return new PhysicalHDFSReader(storage, path);
    }

    @Override
    public boolean compatibleWith(Storage.Scheme scheme)
    {
        return scheme.equals(Storage.Scheme.hdfs);
    }
}
