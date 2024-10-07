package io.pixelsdb.pixels.daemon.monitor.util;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.*;

public class ScalingManager {
    private static final Logger log = LogManager
            .getLogger(ScalingManager.class);
    private final Ec2Manager ecManager;
    private static String amiId;
    private static String keyName;
    private static final String PREFIX = "Auto-EC2";
    private Map<String, InstanceState> instanceMap;

    enum InstanceState {
        STOPPED,
        RUNNING
    }

    public ScalingManager() {
        ConfigFactory config = ConfigFactory.Instance();
        amiId = config.getProperty("vm.ami.id");
        keyName = config.getProperty("vm.key.name");
        List<String> securityGroups = Arrays.asList(config.getProperty("vm.security.groups").split(","));
        String accessKeyId = config.getProperty("vm.access.key.id");
        String secretAccessKey = config.getProperty("vm.secret.access.key");
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                accessKeyId,
                secretAccessKey);
        Ec2Client ec2Client = Ec2Client.builder()
                .region(Region.US_EAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        ecManager = new Ec2Manager(ec2Client, amiId, keyName, securityGroups);
        initInstanceMap(ec2Client);
    }

    private void initInstanceMap(Ec2Client ec2) {
        instanceMap = new HashMap<String, InstanceState>();
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(10).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        if (!instance.imageId().equals(amiId) || !instance.keyName().equals(keyName) || !instance.tags().get(0).value().startsWith(PREFIX)) {
                            break;
                        }
                        switch (instance.state().name()) {
                            case RUNNING: {
                                instanceMap.put(instance.instanceId(), InstanceState.RUNNING);
                                break;
                            }
                            case STOPPED: {
                                instanceMap.put(instance.instanceId(), InstanceState.STOPPED);
                                break;
                            }
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    InstanceState instanceState(String id) {
        return instanceMap.get(id);
    }
    
    public void expandOne() {
        for (String id : instanceMap.keySet()) {
            if (instanceState(id).equals(InstanceState.STOPPED)) {
                ecManager.startInstance(id);
                return;
            }
        }
        createOneInstanceAndStart();
    }

    void startOne(String id) {
        log.debug("Start a VM, id = " + id);
        ecManager.startInstance(id);
        instanceMap.put(id, InstanceState.RUNNING);
    }

    void stopOne(String id) {
        log.debug("Stop a VM, id = " + id);
        ecManager.stopInstance(id);
        instanceMap.put(id, InstanceState.RUNNING);
    }

    private void createOneInstanceAndStart() {
        String instanceId = ecManager
                .createEC2Instance(PREFIX + UUID.randomUUID()); // create后会自动start
        instanceMap.put(instanceId, InstanceState.RUNNING);
        log.debug("Create a new VM, id = " + instanceId);
        System.out.println("Create Instance successfully. The Amazon EC2 Instance ID is " + instanceId);
    }

    public void expandSome(int count) {
        if (count <= 0) return;
        List<String> stoppedInstances = new ArrayList<>();
        for (String id : instanceMap.keySet()) {
            if (instanceState(id).equals(InstanceState.STOPPED)) {
                stoppedInstances.add(id);
            }
        }
        int stoppedCount = stoppedInstances.size();
        if (count > stoppedCount) {
            stoppedInstances.forEach(this::startOne);
            int toCreateCount = count - stoppedCount;
            while (toCreateCount > 0) {
                toCreateCount--;
                createOneInstanceAndStart();
            }
        } else {
            for (String id : stoppedInstances) {
                startOne(id);
                count--;
                if (count == 0) break;
            }
        }
    }

    public void reduceOne() {
        if (!instanceMap.isEmpty()) {
            for (String id : instanceMap.keySet()) {
                if (instanceMap.get(id).equals(InstanceState.RUNNING)) {
                    stopOne(id);
                    return;
                }
            }
        } else {
            System.out.println("No instance, can't reduce");
        }
    }

    public void reduceSome(int count) {
        if (count <= 0) return;
        List<String> runningInstances = new ArrayList<>();
        for (String id : instanceMap.keySet()) {
            if (instanceState(id).equals(InstanceState.RUNNING)) {
                runningInstances.add(id);
            }
        }
        int runningCount = Math.min(runningInstances.size(), count);
        for (String id : runningInstances) {
            stopOne(id);
            runningCount--;
            if (runningCount <= 0) break;
        }
    }

    public void reduceAll() {
        for (String id : instanceMap.keySet()) {
            if (instanceMap.get(id).equals(InstanceState.RUNNING)) {
                stopOne(id);
            }
        }
    }

    public void multiplyInstance(float percent) {
        int count = 0;
        for (String id : instanceMap.keySet()) {
            if (instanceMap.get(id) == InstanceState.RUNNING) {
                count++;
            }
        }
        count = Math.round(count * percent) - count;
        if (count >= 0) {
            expandSome(count);
        } else {
            reduceSome(-count);
        }
    }
}
