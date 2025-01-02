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
package io.pixelsdb.pixels.storage.stream;

import io.pixelsdb.pixels.common.physical.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author huasiy
 * @create 2024-11-05
 */
public class StreamProvider implements StorageProvider
{
    @Override
    public Storage createStorage(@Nonnull Storage.Scheme scheme) throws IOException
    {
        if (!this.compatibleWith(scheme))
        {
            throw new IOException("incompatible storage scheme: " + scheme);
        }
        return new Stream();
    }

    @Override
    public PhysicalReader createReader(@Nonnull Storage storage, @Nonnull String path, @Nullable PhysicalReaderOption option) throws IOException
    {
        if (!this.compatibleWith(storage.getScheme()))
        {
            throw new IOException("incompatible storage scheme: " + storage.getScheme());
        }
        return new PhysicalStreamReader(storage, path);
    }

    @Override
    public PhysicalWriter createWriter(@Nonnull Storage storage, @Nonnull String path, @Nonnull PhysicalWriterOption option) throws IOException
    {
        if (!this.compatibleWith(storage.getScheme()))
        {
            throw new IOException("incompatible storage scheme: " + storage.getScheme());
        }
        return new PhysicalStreamWriter(storage, path);
    }

    @Override
    public boolean compatibleWith(@Nonnull Storage.Scheme scheme) { return scheme.equals(Storage.Scheme.httpstream); }
}
