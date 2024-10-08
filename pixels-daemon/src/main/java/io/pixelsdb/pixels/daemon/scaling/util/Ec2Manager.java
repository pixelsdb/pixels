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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Ec2Manager implements InstanceManager
{
    private final Ec2Client ec2;
    private final String keyName;
    private final String amiId;
    private final List<String> securityGroups;
    private static final Logger log = LogManager
            .getLogger(Ec2Manager.class);

    public Ec2Manager()
    {
        ConfigFactory config = ConfigFactory.Instance();
        this.amiId = config.getProperty("vm.ami.id");
        this.keyName = config.getProperty("vm.key.name");
        this.securityGroups = Arrays.asList(config.getProperty("vm.security.groups").split(","));
        String accessKeyId = config.getProperty("vm.access.key.id");
        String secretAccessKey = config.getProperty("vm.secret.access.key");
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                accessKeyId,
                secretAccessKey);
        this.ec2 = Ec2Client.builder()
                .region(Region.US_EAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
    }

    @Override
    public String createInstance(String name)
    {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.M5_2_XLARGE)
                .maxCount(1)
                .minCount(1)
                .keyName(keyName)
                .securityGroups(securityGroups)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try
        {
            ec2.createTags(tagRequest);
            log.info("Successfully started EC2 Instance {} based on AMI {}",
                    instanceId, amiId);
            return instanceId;
        } catch (Ec2Exception e)
        {
            log.error(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    @Override
    public void startInstance(String instanceId)
    {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(request);
        log.info("Successfully started instance {}", instanceId);
    }

    @Override
    public void stopInstance(String instanceId)
    {
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.stopInstances(request);
        log.info("Successfully stopped instance {}", instanceId);
    }


    @Override
    public Map<String, ScalingManager.InstanceState> initInstanceStateMap()
    {
        Map<String, ScalingManager.InstanceState> instanceMap = new HashMap<>();
        String nextToken = null;
        try
        {
            do
            {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(10).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations())
                {
                    for (Instance instance : reservation.instances())
                    {
                        if (!instance.imageId().equals(amiId) || !instance.keyName().equals(keyName) || !instance.tags().get(0).value().startsWith(PREFIX))
                        {
                            break;
                        }
                        switch (instance.state().name())
                        {
                            case RUNNING:
                            {
                                instanceMap.put(instance.instanceId(), ScalingManager.InstanceState.RUNNING);
                                break;
                            }
                            case STOPPED:
                            {
                                instanceMap.put(instance.instanceId(), ScalingManager.InstanceState.STOPPED);
                                break;
                            }
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e)
        {
            log.error(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return instanceMap;
    }

    public void rebootEC2Instance(String instanceId)
    {
        try
        {
            RebootInstancesRequest request = RebootInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.rebootInstances(request);
            log.info("Successfully rebooted instance {}", instanceId);
        } catch (Ec2Exception e)
        {
            log.error(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void describeImages(Ec2Client ec2, String imageId)
    {
        DescribeImagesRequest request = DescribeImagesRequest.builder()
                .imageIds(imageId)
                .build();

        DescribeImagesResponse response = ec2.describeImages(request);
        response.images().forEach(image -> {
            log.info("Found image with ID {}, and name {}",
                    image.imageId(), image.name());
        });
    }

    public void describeEC2Instances()
    {
        String nextToken = null;
        try
        {
            do
            {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(10).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                log.info("Instance size is " + response.reservations().size());
                log.info("-------------------");
                for (Reservation reservation : response.reservations())
                {
                    for (Instance instance : reservation.instances())
                    {
                        log.info("Instance Id is " + instance.instanceId());
                        log.info("Image id is " + instance.imageId());
                        log.info("Instance type is " + instance.instanceType());
                        log.info("Instance state name is " + instance.state().name());
                        log.info("monitoring information is " + instance.monitoring().state());
                    }
                    log.info("-------------------");
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e)
        {
            log.info(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void close()
    {
        ec2.close();
    }
}
