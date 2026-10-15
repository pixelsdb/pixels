/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.storage.s3qs;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Identifies one physical partition queue within one shuffle edge.
 *
 * Partition ids are only unique inside a shuffle. Including shuffleId keeps
 * the two sides of a join and concurrent queries isolated in a warm worker JVM.
 */
public final class ShuffleQueueKey
{
    private final String shuffleId;
    private final int partitionId;

    public ShuffleQueueKey(String shuffleId, int partitionId)
    {
        this.shuffleId = requireNonNull(shuffleId, "shuffleId is null").trim();
        checkArgument(!this.shuffleId.isEmpty(), "shuffleId is empty");
        checkArgument(partitionId >= 0, "partitionId is negative");
        this.partitionId = partitionId;
    }

    public String getShuffleId()
    {
        return shuffleId;
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (!(other instanceof ShuffleQueueKey))
        {
            return false;
        }
        ShuffleQueueKey that = (ShuffleQueueKey) other;
        return partitionId == that.partitionId && shuffleId.equals(that.shuffleId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(shuffleId, partitionId);
    }

    @Override
    public String toString()
    {
        return shuffleId + ":" + partitionId;
    }
}
