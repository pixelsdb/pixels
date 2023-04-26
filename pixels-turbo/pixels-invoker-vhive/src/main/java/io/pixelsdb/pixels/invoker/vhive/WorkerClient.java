package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerClient {
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub stub;

    public WorkerClient(String host, int port) {
        checkArgument(host != null, "illegal rpc host");
        ;
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = WorkerServiceGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(Option.builder("i")
                .longOpt("interactionId")
                .hasArg(true)
                .desc("interaction id ([REQUIRED] or use --clientId)")
                .required(false)
                .build());
        options.addOption(Option.builder("c")
                .longOpt("clientId")
                .hasArg(true)
                .desc("client id ([REQUIRED] or use --interactionId)")
                .required(false)
                .build());
        options.addOption(Option.builder("f")
                .longOpt("file")
                .hasArg(true)
                .desc("[REQUIRED] one log-file or list of many log-files as input for log-parser")
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .required()
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("c")) {
                String clientId = cmd.getOptionValue("c");
                System.out.println("We have --clientId option = " + clientId);
                if (cmd.hasOption("i")) {
                    System.out.println("( --interactionId option is omitted because --clientId option is defined)");
                }
            } else if (cmd.hasOption("i")) {
                String interactionId = cmd.getOptionValue("i");
                System.out.println("We have --interactionId option " + interactionId);
                if (cmd.hasOption("c")) {
                    System.out.println("( --clientId option is omitted because --interactionId option is defined)");
                }
            } else {
                System.out.println("please specify one of the command line options: "
                        + " -c,--c <arg>             client id\n"
                        + "OR\n"
                        + " -i,--interaction <arg>   interaction id");
            }
            if (cmd.hasOption("f")) {
                String[] files = cmd.getOptionValues("f");
                System.out.println("Number of files: " + files.length);
                System.out.println("FileName(s): " + String.join(", ", Arrays.asList(files)));
            }
        } catch (ParseException pe) {
            System.out.println("Error parsing command-line arguments!");
            System.out.println("Please, follow the instructions below:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log messages to sequence diagrams converter", options);
            System.exit(1);
        }
    }

    public void shutdown() throws InterruptedException {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public ConnectivityState getState() throws InterruptedException {
        return this.channel.getState(true);
    }

    public String hello(String username) {
        WorkerProto.HelloRequest request = WorkerProto.HelloRequest.newBuilder()
                .setName(username)
                .build();

        WorkerProto.HelloResponse response = this.stub.hello(request);
        return response.getOutput();
    }

    public AggregationOutput aggregation(AggregationInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        WorkerProto.WorkerResponse response = this.stub.aggregation(request);
        AggregationOutput output = JSON.parseObject(response.getJson(), AggregationOutput.class);
        return output;
    }

    public JoinOutput broadcastChainJoin(BroadcastChainJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        WorkerProto.WorkerResponse response = this.stub.broadcastChainJoin(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput broadcastJoin(BroadcastJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        WorkerProto.WorkerResponse response = this.stub.broadcastJoin(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput partitionChainJoin(PartitionedChainJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        WorkerProto.WorkerResponse response = this.stub.partitionChainJoin(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public JoinOutput partitionJoin(PartitionedJoinInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        WorkerProto.WorkerResponse response = this.stub.partitionJoin(request);
        JoinOutput output = JSON.parseObject(response.getJson(), JoinOutput.class);
        return output;
    }

    public PartitionOutput partition(PartitionInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();
        WorkerProto.WorkerResponse response = this.stub.partition(request);
        PartitionOutput output = JSON.parseObject(response.getJson(), PartitionOutput.class);
        return output;
    }

    public ScanOutput scan(ScanInput input) {
        WorkerProto.WorkerRequest request = WorkerProto.WorkerRequest.newBuilder()
                .setJson(JSON.toJSONString(input))
                .build();

        WorkerProto.WorkerResponse response = this.stub.scan(request);
        ScanOutput output = JSON.parseObject(response.getJson(), ScanOutput.class);
        return output;
    }
}
