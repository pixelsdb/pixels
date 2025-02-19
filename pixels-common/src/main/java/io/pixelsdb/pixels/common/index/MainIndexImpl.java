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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.index.IndexProto;

import java.io.IOException;

/**
 * @author hank
 * @create 2025-02-19
 */
public class MainIndexImpl implements MainIndex
{
    // TODO: implement

    @Override
    public IndexProto.RowLocation getLocation(long rowId)
    {
        return null;
    }

    @Override
    public boolean putRowIdsOfRg(RowIdRange rowIdRangeOfRg, RgLocation rgLocation)
    {
        return false;
    }

    @Override
    public boolean deleteRowIdRange(RowIdRange rowIdRange)
    {
        return false;
    }

    @Override
    public boolean persist()
    {
        return false;
    }

    @Override
    public void close() throws IOException
    {

    }
}
