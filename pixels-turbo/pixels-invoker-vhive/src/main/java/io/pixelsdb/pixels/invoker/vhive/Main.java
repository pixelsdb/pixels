package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import org.apache.commons.cli.*;

public class Main {
    // here are the default values if not specified in args
    private static final String HOST = "localhost";
    private static final int PORT = 50051;
    private static final String FUNC = "Hello";

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(Option.builder("h")
                .longOpt("host")
                .hasArg(true)
                .desc("GRPC host ([REQUIRED] or use --host)")
                .required(false)
                .build());
        options.addOption(Option.builder("p")
                .longOpt("port")
                .hasArg(true)
                .desc("GRPC port ([REQUIRED] or use --port)")
                .required(false)
                .build());
        options.addOption(Option.builder("f")
                .longOpt("function")
                .hasArg(true)
                .desc("GRPC function ([REQUIRED] or use --function)")
                .required(false)
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            String host = cmd.getOptionValue("host", HOST);
            String port = cmd.getOptionValue("port", Integer.toString(PORT));
            String function = cmd.getOptionValue("function", FUNC);

            WorkerSyncClient client = new WorkerSyncClient(host, Integer.parseInt(port));
            StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, null, null, null);
            Output output = null;
            switch (function) {
//                case "Aggregation":
//                    output = client.aggregation(Utils.genAggregationInput());
//                    break;
//                case "BroadcastChainJoin":
//                    output = client.broadcastChainJoin(Utils.genBroadcastChainJoinInput());
//                    break;
//                case "BroadcastJoin":
//                    output = client.broadcastJoin(Utils.genBroadcastJoinInput());
//                    break;
//                case "PartitionChainJoin":
//                    output = client.partitionChainJoin(Utils.genPartitionedChainJoinInput());
//                    break;
//                case "PartitionJoin":
//                    output = client.partitionJoin(Utils.genPartitionedJoinInput());
//                    break;
//                case "Partition":
//                    output = client.partition(Utils.genPartitionInput("order").apply(0));
//                    break;
                case "Scan":
                    output = client.scan(Utils.genScanInput(storageInfo, 0));
                    break;
                case "Hello":
                    System.out.println(client.hello("zhaoshihan"));
                    break;
                default:
                    throw new ParseException("invalid function name");
            }
            System.out.println(JSON.toJSONString(output));
        } catch (ParseException pe) {
            System.out.println("Error parsing command-line arguments!");
            System.out.println("Please, follow the instructions below:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log messages to sequence diagrams converter", options);
            System.exit(1);
        }
    }
}
