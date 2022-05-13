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

import com.google.common.collect.ImmutableMap;
import io.pixelsdb.pixels.common.exception.BalancerException;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Created at: 19-7-28
 * Author: hank
 */
public class AbsoluteBalancer extends Balancer
{
    private int totalCount = 0;
    private Map<HostAddress, Integer> nodeCounters = new HashMap<>();
    private Map<String, HostAddress> pathToAddress = new HashMap<>();

    @Override
    public void put(String path, HostAddress address)
    {
        requireNonNull(path, "path is null");
        requireNonNull(address, "address is null");
        if (this.nodeCounters.containsKey(address))
        {
            this.nodeCounters.put(address, this.nodeCounters.get(address)+1);
        }
        else
        {
            this.nodeCounters.put(address, 1);
        }
        this.pathToAddress.put(path, address);
        this.totalCount++;
    }

    @Override
    public void put(String path, Set<HostAddress> addresses)
    {
        throw new UnsupportedOperationException("not supported in AbsoluteBalancer");
    }

    @Override
    public void autoSelect(String path)
    {
        throw new UnsupportedOperationException("not supported in AbsoluteBalancer");
    }

    @Override
    public HostAddress get(String path)
    {
        return this.pathToAddress.get(path);
    }

    @Override
    public Map<String, HostAddress> getAll()
    {
        ImmutableMap<String, HostAddress> all = ImmutableMap.copyOf(pathToAddress);
        return all;
    }

    @Override
    public void balance() throws BalancerException
    {
        //int ceil = (int) Math.ceil((double)this.totalCount / (double)this.nodeCounters.size());
        int floor = (int) Math.floor((double)this.totalCount / (double)this.nodeCounters.size());
        int ceil = floor + 1;

        List<HostAddress> peak = new ArrayList<>();
        List<HostAddress> valley = new ArrayList<>();

        for (Map.Entry<HostAddress, Integer> entry : this.nodeCounters.entrySet())
        {
            if (entry.getValue() >= ceil)
            {
                peak.add(entry.getKey());
            }

            if (entry.getValue() < floor)
            {
                valley.add(entry.getKey());
            }
        }

        boolean balanced = false;

        while (balanced == false)
        {
            // we try to move elements from peaks to valleys.
            if (peak.isEmpty() || valley.isEmpty())
            {
                break;
            }
            HostAddress peakAddress = peak.get(0);
            HostAddress valleyAddress = valley.get(0);
            if (this.nodeCounters.get(peakAddress) < ceil)
            {
                // by this.nodeCounters.get(peakAddress) < ceil,
                // we try the best to empty the peaks.
                peak.remove(peakAddress);
                continue;
            }
            if (this.nodeCounters.get(valleyAddress) >= floor)
            {
                valley.remove(valleyAddress);
                continue;
            }
            this.nodeCounters.put(peakAddress, this.nodeCounters.get(peakAddress)-1);
            this.nodeCounters.put(valleyAddress, this.nodeCounters.get(valleyAddress)+1);

            for (Map.Entry<String, HostAddress> entry : this.pathToAddress.entrySet())
            {
                if (entry.getValue().equals(peakAddress))
                {
                    this.pathToAddress.put(entry.getKey(), valleyAddress);
                    break;
                }
            }

            balanced = this.isBalanced();
        }

        if (peak.isEmpty() == false && balanced == false)
        {
            if (valley.isEmpty() == false)
            {
                throw new BalancerException("vally is not empty in the final balancing stage.");
            }

            for (Map.Entry<HostAddress, Integer> entry : this.nodeCounters.entrySet())
            {
                if (entry.getValue() <= floor)
                {
                    valley.add(entry.getKey());
                }
            }

            while (balanced == false)
            {
                // we try to move elements from peaks to valleys.
                if (peak.isEmpty() || valley.isEmpty())
                {
                    break;
                }
                HostAddress peakAddress = peak.get(0);
                HostAddress valleyAddress = valley.get(0);
                if (this.nodeCounters.get(peakAddress) < ceil)
                {
                    // by this.nodeCounters.get(peakAddress) < ceil,
                    // we try the best to empty the peaks.
                    peak.remove(peakAddress);
                    continue;
                }
                if (this.nodeCounters.get(valleyAddress) > floor)
                {
                    valley.remove(valleyAddress);
                    continue;
                }
                this.nodeCounters.put(peakAddress, this.nodeCounters.get(peakAddress)-1);
                this.nodeCounters.put(valleyAddress, this.nodeCounters.get(valleyAddress)+1);

                for (Map.Entry<String, HostAddress> entry : this.pathToAddress.entrySet())
                {
                    if (entry.getValue().equals(peakAddress))
                    {
                        this.pathToAddress.put(entry.getKey(), valleyAddress);
                        break;
                    }
                }

                balanced = this.isBalanced();
            }
        }
    }

    @Override
    public boolean isBalanced()
    {
        int ceil = (int) Math.ceil((double)this.totalCount / (double)this.nodeCounters.size());
        int floor = (int) Math.floor((double)this.totalCount / (double)this.nodeCounters.size());

        boolean balanced = true;
        for (Map.Entry<HostAddress, Integer> entry : this.nodeCounters.entrySet())
        {
            if (entry.getValue() > ceil || entry.getValue() < floor)
            {
                balanced = false;
            }
        }

        return balanced;
    }
}
