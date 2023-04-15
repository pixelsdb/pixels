/*
 * Copyright 2021-2023 PixelsDB.
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

import java.io.File;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2021-08-20 (LocalFS.Path)
 * @update 2023-04-15 (moved to FilePath)
 */
public class FilePath
{
    public String realPath = null;
    public boolean valid = false;
    public boolean isDir = false;

    public FilePath(String path)
    {
        requireNonNull(path);
        if (path.startsWith("file:///"))
        {
            valid = true;
            realPath = path.substring(path.indexOf("://") + 3);
        }
        else if (path.startsWith("/"))
        {
            valid = true;
            realPath = path;
        }

        if (valid)
        {
            File file = new File(realPath);
            isDir = file.isDirectory();
        }
    }

    @Override
    public String toString()
    {
        if (!this.valid)
        {
            return null;
        }
        return this.realPath;
    }

    /**
     * Convert this path to a String with the scheme prefix of the storage.
     * @param storage the storage.
     * @return the String form of the path.
     * @throws IOException
     */
    public String toStringWithPrefix(Storage storage) throws IOException
    {
        if (!this.valid)
        {
            return null;
        }
        return storage.ensureSchemePrefix(this.realPath);
    }
}
