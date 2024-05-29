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
package io.pixelsdb.pixels.common.metadata.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * The key columns if a range index (partition).
 * It can be used as the object parsed from the json string stored in metadata RANGE_INDEXES.RI_KEY_COLS.
 * @author hank
 * @create 2024-05-22
 */
public class KeyColumns
{
    private List<Integer> keyColumnIds = new ArrayList<>();

    public List<Integer> getKeyColumnIds()
    {
        return keyColumnIds;
    }

    public void setKeyColumnIds(List<Integer> keyColumnIds)
    {
        this.keyColumnIds = keyColumnIds;
    }

    public void addKeyColumnIds(int keyColumnId)
    {
        this.keyColumnIds.add(keyColumnId);
    }
}
