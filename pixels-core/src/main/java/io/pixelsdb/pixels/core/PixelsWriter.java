/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.Closeable;
import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface PixelsWriter
        extends Closeable
{
    /**
     * Add row batch into the file.
     *
     * @return if the file adds a new row group, return false. Else, return true.
     */
    boolean addRowBatch(VectorizedRowBatch rowBatch)
            throws IOException;

    /**
     * Get schema of this file.
     *
     * @return schema
     */
    TypeDescription getSchema();
}
