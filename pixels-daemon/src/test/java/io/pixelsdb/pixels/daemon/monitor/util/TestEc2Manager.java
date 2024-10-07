package io.pixelsdb.pixels.daemon.monitor.util;

import org.junit.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TestEc2Manager {
    static Ec2Client ec2Client;
    static Ec2Manager ec2Manager;
    static final String instanceId = "i-INSTANCCE-ID";
    static final String KEY_NAME = "pixels";
    static final List<String> SECURITY_GROUPS = Arrays.asList("pixels-sg1");
    static final String AMI_ID = "ami-WORKER-AMI_ID";
    static final String ACCESS_KEY_ID = "ACCESS-KEY-ID";
    static final String SECRET_ACCESS_KEY = "SECRET-ACCESS-KEY";

    @BeforeClass
    public static void beforeCreateInstance() {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                ACCESS_KEY_ID,
                SECRET_ACCESS_KEY);
        ec2Client = Ec2Client.builder()
                .region(Region.US_EAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        ec2Manager = new Ec2Manager(ec2Client, AMI_ID, KEY_NAME, SECURITY_GROUPS);
    }

    @Test
    public void testDescribeImages() {
        Ec2Manager.describeImages(ec2Client, AMI_ID);
    }

    @Test
    public void testCreateInstance() {
        String instanceId = ec2Manager
                .createEC2Instance("Auto-EC2-" + UUID.randomUUID());
        System.out.println("Create Instance successfully. The Amazon EC2 Instance ID is " + instanceId);
    }

    @Test
    public void testStopInstance() {
        ec2Manager.stopInstance(instanceId);
    }

    @Test
    public void testStartInstance() {
        ec2Manager.startInstance(instanceId);
    }

    @Test
    public void testRebootInstance() {
        ec2Manager.rebootEC2Instance(instanceId);
    }

    @Test
    public void testDescribeEC2Instances() {
        ec2Manager.describeEC2Instances();
    }

    @AfterClass
    public static void close() {
        ec2Manager.close();
    }
}
