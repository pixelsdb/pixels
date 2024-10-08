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

import org.junit.*;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.util.UUID;

public class TestEc2Manager
{
    static Ec2Client ec2Client;
    static Ec2Manager ec2Manager;
    static final String instanceId = "i-INSTANCCE-ID";
    static final String AMI_ID = "ami-WORKER-AMI_ID";

    @BeforeClass
    public static void beforeCreateInstance()
    {
        ec2Manager = new Ec2Manager();
    }

    @Test
    public void testDescribeImages()
    {
        Ec2Manager.describeImages(ec2Client, AMI_ID);
    }

    @Test
    public void testCreateInstance()
    {
        String instanceId = ec2Manager.createInstance(InstanceManager.PREFIX + UUID.randomUUID());
    }

    @Test
    public void testStopInstance()
    {
        ec2Manager.stopInstance(instanceId);
    }

    @Test
    public void testStartInstance()
    {
        ec2Manager.startInstance(instanceId);
    }

    @Test
    public void testRebootInstance()
    {
        ec2Manager.rebootEC2Instance(instanceId);
    }

    @Test
    public void testDescribeEC2Instances()
    {
        ec2Manager.describeEC2Instances();
    }

    @AfterClass
    public static void close()
    {
        ec2Manager.close();
    }
}
