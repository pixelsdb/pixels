/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.common.balance;

import io.pixelsdb.pixels.common.exception.BalancerException;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Created at: 19-7-28
 * Author: hank
 */
public abstract class Balancer
{
    abstract public void put (String path, HostAddress address);
    abstract public void put (String path, Set<HostAddress> addresses);

    /**
     * Automatically select a location for the given path, using policies such as round-robin.
     * This method can be used the select the cache location for the storage systems that do
     * not provide data locality.
     * @param path
     */
    abstract public void autoSelect (String path); // Added in Issue #222.
    abstract public HostAddress get (String path);
    abstract public Map<String, HostAddress> getAll();
    abstract public void balance () throws BalancerException;
    abstract public boolean isBalanced ();

    /**
     * Pass the balanced path-locations of this balancer to the other balancer.
     * @param other
     */
    public void cascade(Balancer other)
    {
        requireNonNull(other, "the other balancer is null");
        Map<String, HostAddress> all = this.getAll();
        checkArgument(all != null && !all.isEmpty(),
                "the current balancer has no path-locations to be balanced");

        for (Map.Entry<String, HostAddress> entry : all.entrySet())
        {
            other.put(entry.getKey(), entry.getValue());
        }
    }
}
