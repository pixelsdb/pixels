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

import java.util.Objects;

public class LocationIdentifier
{
    public enum LocationType
    {
        ACTIVE_MEMTABLE,
        IMMUTABLE_MEMTABLE,
        ETCD_ENTRY
    }
    private final LocationType type;
    
    /**
     * The identifier of the location.
     * 
     * For ACTIVE_MEMTABLE/IMMUTABLE_MEMTABLE, it is the memory address of the memtable.
     * For ETCD_ENTRY, it is the id of the etcd entry.
     */
    private final String identifier;

    public LocationIdentifier(LocationType type, String identifier)
    {
        this.type = type;
        this.identifier = identifier;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocationIdentifier that = (LocationIdentifier) o;
        return type == that.type && identifier.equals(that.identifier);
    }
    
    @Override
    public int hashCode()
    {
        return Objects.hash(type, identifier);
    }
}
