package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Main {
    // here are the default values if not specified in args
    private static final String HOST = "localhost";
    private static final int PORT = 50051;
    private static final String FUNC = "Hello";
    private static final int NUMBER = 1;

    public static void main(String[] args) throws InterruptedException {
        Options options = new Options();
        options.addOption(Option.builder("h")
                .longOpt("host")
                .hasArg(true)
                .desc("GRPC host (or use --host)")
                .required(false)
                .build());
        options.addOption(Option.builder("p")
                .longOpt("port")
                .hasArg(true)
                .desc("GRPC port (or use --port)")
                .required(false)
                .build());
        options.addOption(Option.builder("f")
                .longOpt("function")
                .hasArg(true)
                .desc("GRPC function (or use --function)")
                .required(false)
                .build());
        options.addOption(Option.builder("n")
                .longOpt("number")
                .hasArg(true)
                .desc("GRPC same request number (or use --number)")
                .required(false)
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            String host = cmd.getOptionValue("host", HOST);
            String port = cmd.getOptionValue("port", Integer.toString(PORT));
            String function = cmd.getOptionValue("function", FUNC);
            String number = cmd.getOptionValue("number", Integer.toString(NUMBER));

            StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, null, null, null);
            InvokerFactory factory = InvokerFactory.Instance();

            CountDownLatch countDownLatch = new CountDownLatch(Integer.parseInt(number));
            List<Long> times = new ArrayList<>();
//            WorkerAsyncClient client = new WorkerAsyncClient(host, Integer.parseInt(port));

            for (int i = 0; i < Integer.parseInt(number); ++i) {
                final CompletableFuture<Output> completableFuture;
                long startTime = System.nanoTime();
                switch (function) {
                    case "Aggregation":
                        completableFuture = factory.getInvoker(WorkerType.AGGREGATION).invoke(Utils.genAggregationInput(storageInfo));
                        break;
                    case "BroadcastChainJoin":
                        completableFuture = factory.getInvoker(WorkerType.BROADCAST_CHAIN_JOIN).invoke(Utils.genBroadcastChainJoinInput(storageInfo));
                        break;
                    case "BroadcastJoin":
                        completableFuture = factory.getInvoker(WorkerType.BROADCAST_JOIN).invoke(Utils.genBroadcastJoinInput(storageInfo));
                        break;
                    case "PartitionChainJoin":
                        completableFuture = factory.getInvoker(WorkerType.PARTITIONED_CHAIN_JOIN).invoke(Utils.genPartitionedChainJoinInput(storageInfo));
                        break;
                    case "PartitionJoin":
                        completableFuture = factory.getInvoker(WorkerType.PARTITIONED_JOIN).invoke(Utils.genPartitionedJoinInput(storageInfo));
                        break;
                    case "Partition":
                        assert Utils.genPartitionInput("order") != null;
                        completableFuture = factory.getInvoker(WorkerType.PARTITION).invoke(Utils.genPartitionInput("order").apply(storageInfo, 0));
                        break;
                    case "Scan":
                        completableFuture = factory.getInvoker(WorkerType.SCAN).invoke(Utils.genScanInput(storageInfo, 0));
                        break;
                    default:
                        throw new ParseException("invalid function name");
                }
                if (completableFuture != null) {
                    Thread futureThread = new Thread(() -> {
                        try {
                            Output output = completableFuture.get();
                            long endTime = System.nanoTime();
                            synchronized (System.out) {
                                System.out.println(JSON.toJSONString(output));
                                System.out.println("Entire round trip time(MS): " + (endTime - startTime) / 1000000);
                                System.out.println();

                                times.add((endTime - startTime) / 1000000);
                            }
                            countDownLatch.countDown();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    futureThread.start();
                }
            }
            countDownLatch.await();
            LongSummaryStatistics statistics = times.stream().mapToLong((x) -> x).summaryStatistics();
            System.out.println(statistics);
        } catch (ParseException pe) {
            System.out.println("Error parsing command-line arguments!");
            System.out.println("Please, follow the instructions below:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log messages to sequence diagrams converter", options);
            System.exit(1);
        }
    }
}
