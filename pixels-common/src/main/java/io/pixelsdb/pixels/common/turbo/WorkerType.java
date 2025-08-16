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
package io.pixelsdb.pixels.common.turbo;

/**
 * Worker type is the type of cloud function for query execution. In pixels-turbo, each physical query operator is
 * implemented as a cloud function, each has a corresponding worker type.
 * @author hank
 * @create 2022-06-28
 */
public enum WorkerType
{
    UNKNOWN, // The first enum value is the default value.
    SCAN, SCAN_STREAMING,
    PARTITION, PARTITION_STREAMING,
    BROADCAST_JOIN, BROADCAST_JOIN_STREAMING,
    BROADCAST_CHAIN_JOIN, BROADCAST_CHAIN_JOIN_STREAMING,
    PARTITIONED_JOIN, PARTITIONED_JOIN_STREAMING,
    PARTITIONED_CHAIN_JOIN, PARTITIONED_CHAIN_JOIN_STREAMING,
    SORT,
    SORTED_JOIN,
    AGGREGATION;

    public static WorkerType from(String value)
    {
        return valueOf(value.toUpperCase());
    }

    public boolean equals(String other)
    {
        return this.toString().equalsIgnoreCase(other);
    }

    public boolean equals(WorkerType other)
    {
        // enums in Java can be compared using '=='.
        return this == other;
    }
}
