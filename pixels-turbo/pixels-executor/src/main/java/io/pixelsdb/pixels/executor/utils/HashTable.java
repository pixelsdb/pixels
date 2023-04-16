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
package io.pixelsdb.pixels.executor.utils;

import java.util.*;

/**
 * @author hank
 * @date 31/05/2022
 */
public class HashTable implements Iterable<Tuple>
{
    private final int NUM_HASH_TABLES = 41;
    private final List<HashMap<Tuple, Tuple>> hashTables;
    private int size = 0;

    public HashTable()
    {
        this.hashTables = new ArrayList<>(NUM_HASH_TABLES);
        for (int i = 0; i < NUM_HASH_TABLES; ++i)
        {
            this.hashTables.add(new HashMap<>());
        }
    }

    /**
     * The user of this method must ensure the tuple is not null and the same tuple
     * is only put once. Different tuples with the same value of join key are put
     * into the same bucket.
     *
     * @param tuple the tuple to be put
     */
    public void put(Tuple tuple)
    {
        size++;
        int index = tuple.hashCode() % NUM_HASH_TABLES;
        Map<Tuple, Tuple> hashTable = this.hashTables.get(index < 0 ? -index : index);
        Tuple head = hashTable.get(tuple);
        if (head != null)
        {
            tuple.next = head.next;
            head.next = tuple;
            return;
        }
        hashTable.put(tuple, tuple);
    }

    public Tuple getHead(Tuple tuple)
    {
        int index = tuple.hashCode() % NUM_HASH_TABLES;
        return this.hashTables.get(index < 0 ? -index : index).get(tuple);
    }

    public int size()
    {
        return size;
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return new TupleIterator();
    }

    private class TupleIterator implements Iterator<Tuple>
    {
        private int mapIndex;
        private Iterator<Tuple> mapIterator;
        private Tuple currHead = null;

        private TupleIterator()
        {
            mapIndex = 0;
            mapIterator = hashTables.get(mapIndex++).values().iterator();
            if (mapIterator.hasNext())
            {
                currHead = mapIterator.next();
            }
        }

        @Override
        public boolean hasNext()
        {
            return currHead != null || mapIterator.hasNext() || mapIndex < NUM_HASH_TABLES;
        }

        @Override
        public Tuple next()
        {
            Tuple next;
            if (currHead != null)
            {
                next = currHead;
                currHead = currHead.next;
                return next;
            }
            if (mapIterator.hasNext())
            {
                next = mapIterator.next();
                currHead = next.next;
                return next;
                // We ensure there is no null value in the hash maps.
            }
            if (mapIndex < NUM_HASH_TABLES)
            {
                mapIterator = hashTables.get(mapIndex++).values().iterator();
                if (mapIterator.hasNext())
                {
                    next = mapIterator.next();
                    currHead = next.next;
                    return next;
                }
            }
            throw new NoSuchElementException("no more tuples");
        }
    }
}
