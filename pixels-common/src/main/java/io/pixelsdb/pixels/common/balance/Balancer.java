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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.balance;

import com.facebook.presto.spi.HostAddress;
import io.pixelsdb.pixels.common.exception.BalancerException;

import java.util.Map;
import java.util.Set;

/**
 * Created at: 19-7-28
 * Author: hank
 */
public abstract class Balancer
{
    abstract public void put (String path, HostAddress address);
    abstract public void put (String path, Set<HostAddress> addresses);
    abstract public HostAddress get (String path);
    abstract public Map<String, HostAddress> getAll();
    abstract public void balance () throws BalancerException;
    abstract public boolean isBalanced ();

    public void cascade(Balancer balancer)
    {
        if (balancer == null)
        {
            return;
        }

        Map<String, HostAddress> all = this.getAll();

        if (all != null)
        {
            for (Map.Entry<String, HostAddress> entry : all.entrySet())
            {
                balancer.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
