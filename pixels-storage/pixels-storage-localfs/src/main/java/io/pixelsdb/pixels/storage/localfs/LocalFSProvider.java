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
package io.pixelsdb.pixels.storage.localfs;

import io.pixelsdb.pixels.common.physical.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author hank
 * @create 2023-04-15
 */
public class LocalFSProvider implements StorageProvider
{
    @Override
    public Storage createStorage(@Nonnull Storage.Scheme scheme) throws IOException
    {
        if (!this.compatibleWith(scheme))
        {
            throw new IOException("incompatible storage scheme: " + scheme);
        }
        return new LocalFS();
    }

    @Override
    public PhysicalReader createReader(@Nonnull Storage storage, @Nonnull String path, @Nullable PhysicalReaderOption option) throws IOException
    {
        if (!this.compatibleWith(storage.getScheme()))
        {
            throw new IOException("incompatible storage scheme: " + storage.getScheme());
        }
        return new PhysicalLocalReader(storage, path);
    }

    @Override
    public PhysicalWriter createWriter(@Nonnull Storage storage, @Nonnull String path, @Nonnull PhysicalWriterOption option) throws IOException
    {
        if (!this.compatibleWith(storage.getScheme()))
        {
            throw new IOException("incompatible storage scheme: " + storage.getScheme());
        }
        return new PhysicalLocalWriter(storage, path, option.isOverwrite());
    }

    @Override
    public boolean compatibleWith(@Nonnull Storage.Scheme scheme)
    {
        return scheme.equals(Storage.Scheme.file);
    }
}
