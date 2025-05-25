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
package io.pixelsdb.pixels.retina;

import java.util.List;

public class SuperVersion implements Referenceable
{
    private final ReferenceCounter refCounter = new ReferenceCounter();

    private final MemTable memTable;
    private final List<MemTable> immutableMemTables;
    private final List<EtcdEntry> etcdEntries;

    public SuperVersion(
            MemTable memTable,
            List<MemTable> immutableMemTables,
            List<EtcdEntry> etcdEntries)
    {
        this.memTable = memTable;
        this.immutableMemTables  = immutableMemTables;
        this.etcdEntries = etcdEntries;

        this.memTable.ref();
        for (MemTable immutableMemTable : this.immutableMemTables)
        {
            immutableMemTable.ref();
        }
        for (EtcdEntry etcdEntry : this.etcdEntries)
        {
            etcdEntry.ref();
        }
        this.refCounter.ref();
    }

    public MemTable getMemTable()
    {
        return this.memTable;
    }

    public List<MemTable> getImmutableMemTables()
    {
        return this.immutableMemTables;
    }

    public List<EtcdEntry> getEtcdEntries()
    {
        return this.etcdEntries;
    }

    @Override
    public void ref()
    {
        this.refCounter.ref();
    }

    @Override
    public boolean unref()
    {
        boolean shouldDelete = this.refCounter.unref();
        if (shouldDelete)
        {
            this.memTable.unref();
            for (MemTable immutableMemTable : this.immutableMemTables)
            {
                immutableMemTable.unref();
            }
            for (EtcdEntry etcdEntry : this.etcdEntries)
            {
                etcdEntry.unref();
            }
        }
        return shouldDelete;
    }

    @Override
    public int getRefCount()
    {
        return this.refCounter.getCount();
    }
}
