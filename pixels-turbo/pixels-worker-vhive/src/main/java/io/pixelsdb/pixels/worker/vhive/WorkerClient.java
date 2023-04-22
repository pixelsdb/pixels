package io.pixelsdb.pixels.worker.vhive;

import com.alibaba.fastjson.JSON;
import com.google.common.primitives.Booleans;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerClient {
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub stub;

    public WorkerClient(String host, int port) {
        checkArgument(host != null, "illegal rpc host");;
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = WorkerServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public String hello(String username) {
        WorkerProto.HelloRequest request = WorkerProto.HelloRequest.newBuilder()
                .setName(username)
                .build();

        WorkerProto.HelloResponse response = this.stub.hello(request);
        return response.getOutput();
    }


    public ScanOutput scan(ScanInput input) throws IOException {
        String inputJSON = JSON.toJSONString(input);
        WorkerProto.ScanInput.Builder builder = WorkerProto.ScanInput.newBuilder();
        JsonFormat.parser().merge(inputJSON, builder);
        WorkerProto.ScanInput request = builder.build();


        WorkerProto.ScanOutput response = this.stub.scan(request);

        String outputJSON = JsonFormat.printer().print(response);
        ScanOutput output = JSON.parseObject(outputJSON, ScanOutput.class);
        return output;
    }

//    private WorkerProto.ScanInput physicalToProto(ScanInput request) {
//        return WorkerProto.ScanInput.newBuilder()
//                .setQueryId(request.getQueryId())
//                .setScanTableInfo(physicalToProto(request.getTableInfo()))
//                .addAllScanProjection(Booleans.asList(request.getScanProjection()))
//                .setPartialAggregationPresent(request.isPartialAggregationPresent())
//                .setPartialAggregationInfo(request.getPartialAggregationInfo())
//                .setOutput()
//                .build();
//    }
//
//
//    private WorkerProto.ScanTableInfo physicalToProto(ScanTableInfo request) {
//        return WorkerProto.ScanTableInfo.newBuilder()
//                .setTableName(request.getTableName())
//                .setBase(request.isBase())
//                .addAllColumnsToRead(Arrays.asList(request.getColumnsToRead()))
//                .addAllInputSplits(request.getInputSplits().stream()
//                        .map(this::physicalToProto)
//                        .collect(Collectors.toList()))
//                .setFilter(request.getFilter())
//                .build();
//    }
//
//    private WorkerProto.InputSplit physicalToProto(InputSplit request) {
//        return WorkerProto.InputSplit.newBuilder()
//                .addAllInputInfos(request.getInputInfos().stream()
//                    .map(this::physicalToProto)
//                    .collect(Collectors.toList()))
//                .build();
//    }
//
//    private WorkerProto.InputInfo physicalToProto(InputInfo request) {
//        return WorkerProto.InputInfo.newBuilder()
//                .setPath(request.getPath())
//                .setRgStart(request.getRgStart())
//                .setRgLength(request.getRgLength())
//                .build();
//    }
}
