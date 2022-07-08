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
    private final HashMap<Tuple, Tuple> hashTable = new HashMap<>();
    private int size = 0;

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
        Tuple head = this.hashTable.get(tuple);
        if (head != null)
        {
            tuple.next = head.next;
            head.next = tuple;
            return;
        }
        this.hashTable.put(tuple, tuple);
    }

    public Tuple getHead(Tuple tuple)
    {
        return this.hashTable.get(tuple);
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
        private final Iterator<Tuple> mainIterator;
        private Tuple currHead = null;

        private TupleIterator()
        {
            mainIterator = hashTable.values().iterator();
            if (mainIterator.hasNext())
            {
                currHead = mainIterator.next();
            }
        }

        @Override
        public boolean hasNext()
        {
            return currHead != null || mainIterator.hasNext();
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
            if (mainIterator.hasNext())
            {
                currHead = mainIterator.next();
                if (currHead != null)
                {
                    next = currHead;
                    currHead = currHead.next;
                    return next;
                }
            }
            throw new NoSuchElementException("no more tuples");
        }
    }
}
