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
package io.pixelsdb.pixels.storage.gcs;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderOption;
import io.pixelsdb.pixels.common.physical.PhysicalReaderProvider;
import io.pixelsdb.pixels.common.physical.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author hank
 * @create 2023-04-15
 */
public class GCSReaderProvider implements PhysicalReaderProvider
{
    @Override
    public PhysicalReader createReader(@Nonnull Storage storage, @Nonnull String path,
                                       @Nullable PhysicalReaderOption option) throws IOException
    {
        if (!storage.getScheme().equals(Storage.Scheme.gcs))
        {
            throw new IOException("incompatible storage scheme: " + storage.getScheme());
        }
        return new PhysicalGCSReader(storage, path);
    }

    @Override
    public boolean compatibleWith(@Nonnull Storage.Scheme scheme)
    {
        return scheme.equals(Storage.Scheme.gcs);
    }
}
