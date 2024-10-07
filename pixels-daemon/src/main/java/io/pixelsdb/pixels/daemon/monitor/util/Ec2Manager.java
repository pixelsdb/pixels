package io.pixelsdb.pixels.daemon.monitor.util;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.List;

public class Ec2Manager {
    Ec2Client ec2;
    String keyName;
    String amiId;
    List<String> securityGroups;

    public Ec2Manager(Ec2Client client, String amiId, String keyName, List<String> securityGroups) {
        this.ec2 = client;
        this.amiId = amiId;
        this.keyName = keyName;
        this.securityGroups = securityGroups;
    }

    public String createEC2Instance(String name) {
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

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 Instance %s based on AMI %s",
                    instanceId, amiId);
            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public void startInstance(String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(request);
        System.out.printf("Successfully started instance %s", instanceId);
    }

    public void stopInstance(String instanceId) {
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.stopInstances(request);
        System.out.printf("Successfully stopped instance %s", instanceId);
    }

    public void rebootEC2Instance(String instanceId) {
        try {
            RebootInstancesRequest request = RebootInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.rebootInstances(request);
            System.out.printf(
                    "Successfully rebooted instance %s", instanceId);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void describeImages(Ec2Client ec2, String imageId) {
        DescribeImagesRequest request = DescribeImagesRequest.builder()
                .imageIds(imageId)
                .build();

        DescribeImagesResponse response = ec2.describeImages(request);
        response.images().forEach(image -> {
            System.out.printf("Found image with ID %s, and name %s%n",
                    image.imageId(), image.name());
        });
    }

    public void describeEC2Instances() {
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(10).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                System.out.println("Instance size is " + response.reservations().size());
                System.out.println("-------------------");
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        System.out.println("Instance Id is " + instance.instanceId());
                        System.out.println("Image id is " + instance.imageId());
                        System.out.println("Instance type is " + instance.instanceType());
                        System.out.println("Instance state name is " + instance.state().name());
                        System.out.println("monitoring information is " + instance.monitoring().state());
                    }
                    System.out.println("-------------------");
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void close() {
        ec2.close();
    }
}
