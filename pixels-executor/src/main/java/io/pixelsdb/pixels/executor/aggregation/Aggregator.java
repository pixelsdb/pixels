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
package io.pixelsdb.pixels.executor.aggregation;

import java.util.HashMap;

/**
 * @author hank
 * @date 07/07/2022
 */
public class Aggregator
{
    private final HashMap<AggrTuple, AggrTuple> hashTable = new HashMap<>();
    private int size;

    /**
     * The user of this method must ensure the tuple is not null and the same tuple
     * is only put once. Different tuples with the same value of join key are put
     * into the same bucket.
     *
     * @param tuple the tuple to be put
     */
    public void put(AggrTuple tuple)
    {
        AggrTuple baseTuple = this.hashTable.get(tuple);
        if (baseTuple != null)
        {
            baseTuple.aggregate(tuple);
            return;
        }
        size++;
        // Create the functions.
        this.hashTable.put(tuple, tuple);
    }

    public int size()
    {
        return size;
    }
}
