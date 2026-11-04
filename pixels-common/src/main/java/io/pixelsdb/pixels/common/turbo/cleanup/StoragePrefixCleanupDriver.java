/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.common.turbo.cleanup;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import java.io.IOException;
import java.net.URI;

/** Reuses the configured storage plugin; only explicitly owned, non-root folders are accepted. */
public final class StoragePrefixCleanupDriver implements CleanupDriver
{
    public String getType() { return "storage-prefix"; }

    @Override
    public void validate(String location) throws IOException
    {
        try
        {
            URI uri = URI.create(location);
            String path = uri.getPath();
            if (!location.endsWith("/") || uri.getScheme() == null || path == null ||
                    path.length() <= 1 || !uri.normalize().equals(uri) || uri.getQuery() != null ||
                    uri.getFragment() != null)
            { throw new IOException("cleanup requires a non-root folder URI: " + location); }
        }
        catch (IllegalArgumentException failure) { throw new IOException("invalid cleanup URI", failure); }
    }

    private Storage storage(String location) throws IOException
    {
        validate(location);
        return StorageFactory.Instance().getStorage(Storage.Scheme.from(URI.create(location).getScheme()));
    }

    public void delete(String location) throws IOException { storage(location).delete(location, true); }

    public boolean exists(String location) throws IOException
    { return storage(location).exists(location); }
}
