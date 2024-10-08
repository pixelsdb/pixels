/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.daemon.scaling.util;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.*;

public class ScalingManager
{
    private static final Logger log = LogManager
            .getLogger(ScalingManager.class);
    private final VmManager vmManager;
    private Map<String, InstanceState> instanceMap;

    public enum InstanceState
    {
        STOPPED,
        RUNNING
    }

    public ScalingManager()
    {
        ConfigFactory config = ConfigFactory.Instance();
        vmManager = new Ec2Manager();
        instanceMap = vmManager.initInstanceStateMap();
    }

    InstanceState instanceState(String id)
    {
        return instanceMap.get(id);
    }

    public void expandOne()
    {
        for (String id : instanceMap.keySet())
        {
            if (instanceState(id).equals(InstanceState.STOPPED))
            {
                vmManager.startInstance(id);
                return;
            }
        }
        createOneInstanceAndStart();
    }

    private void startOne(String id)
    {
        log.debug("Start a VM, id = " + id);
        vmManager.startInstance(id);
        instanceMap.put(id, InstanceState.RUNNING);
    }

    private void stopOne(String id)
    {
        log.debug("Stop a VM, id = " + id);
        vmManager.stopInstance(id);
        instanceMap.put(id, InstanceState.RUNNING);
    }

    private void createOneInstanceAndStart()
    {
        String instanceId = vmManager
                .createInstance(vmManager.PREFIX + UUID.randomUUID());
        instanceMap.put(instanceId, InstanceState.RUNNING);
        log.debug("Create a new VM, id = " + instanceId);
    }

    public void expandSome(int count)
    {
        if (count <= 0) return;
        List<String> stoppedInstances = new ArrayList<>();
        for (String id : instanceMap.keySet())
        {
            if (instanceState(id).equals(InstanceState.STOPPED))
            {
                stoppedInstances.add(id);
            }
        }
        int stoppedCount = stoppedInstances.size();
        if (count > stoppedCount)
        {
            stoppedInstances.forEach(this::startOne);
            int toCreateCount = count - stoppedCount;
            while (toCreateCount > 0)
            {
                toCreateCount--;
                createOneInstanceAndStart();
            }
        } else
        {
            for (String id : stoppedInstances)
            {
                startOne(id);
                count--;
                if (count == 0) break;
            }
        }
    }

    public void reduceOne()
    {
        if (!instanceMap.isEmpty())
        {
            for (String id : instanceMap.keySet())
            {
                if (instanceMap.get(id).equals(InstanceState.RUNNING))
                {
                    stopOne(id);
                    return;
                }
            }
        } else
        {
            log.error("No instance, can't reduce");
        }
    }

    public void reduceSome(int count)
    {
        if (count <= 0) return;
        List<String> runningInstances = new ArrayList<>();
        for (String id : instanceMap.keySet())
        {
            if (instanceState(id).equals(InstanceState.RUNNING))
            {
                runningInstances.add(id);
            }
        }
        int runningCount = Math.min(runningInstances.size(), count);
        for (String id : runningInstances)
        {
            stopOne(id);
            runningCount--;
            if (runningCount <= 0) break;
        }
    }

    public void reduceAll()
    {
        for (String id : instanceMap.keySet())
        {
            if (instanceMap.get(id).equals(InstanceState.RUNNING))
            {
                stopOne(id);
            }
        }
    }

    public void multiplyInstance(float percent)
    {
        int count = 0;
        for (String id : instanceMap.keySet())
        {
            if (instanceMap.get(id) == InstanceState.RUNNING)
            {
                count++;
            }
        }
        count = Math.round(count * percent) - count;
        if (count >= 0)
        {
            expandSome(count);
        } else
        {
            reduceSome(-count);
        }
    }
}
