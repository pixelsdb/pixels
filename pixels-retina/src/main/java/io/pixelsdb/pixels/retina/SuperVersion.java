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

import java.util.ArrayList;
import java.util.List;

public class SuperVersion implements Referenceable
{
    private final ReferenceCounter refCounter = new ReferenceCounter();

    private final MemTable activeMemTable;
    private final List<MemTable> immutableMemTables;
    private final List<ObjectEntry> objectEntries;

    public SuperVersion(
            MemTable memTable,
            List<MemTable> immutableMemTables,
            List<ObjectEntry> objectEntries)
    {
        this.activeMemTable = memTable;
        this.immutableMemTables  = new ArrayList<>(immutableMemTables);
        this.objectEntries = new ArrayList<>(objectEntries);

        this.activeMemTable.ref();
        for (MemTable immutableMemTable : this.immutableMemTables)
        {
            immutableMemTable.ref();
        }
        for (ObjectEntry objectEntry : this.objectEntries)
        {
            objectEntry.ref();
        }
        this.refCounter.ref();
    }

    public MemTable getActiveMemTable()
    {
        return this.activeMemTable;
    }

    public List<MemTable> getImmutableMemTables()
    {
        return this.immutableMemTables;
    }

    public List<ObjectEntry> getObjectEntries()
    {
        return this.objectEntries;
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
            this.activeMemTable.unref();
            for (MemTable immutableMemTable : this.immutableMemTables)
            {
                immutableMemTable.unref();
            }
            for (ObjectEntry objectEntry : this.objectEntries)
            {
                objectEntry.unref();
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
