package io.pixelsdb.pixels.worker.vhive;

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import io.grpc.stub.StreamObserver;

import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.stream.Collectors;

public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
    private static Logger log = LogManager.getLogger(WorkerServiceImpl.class);
    private final ScanWorker scanWorker;

    public WorkerServiceImpl() {
        this.scanWorker = new ScanWorker();
    }

    @Override
    public void scan(WorkerProto.ScanRequest request, StreamObserver<WorkerProto.ScanResponse> responseObserver) {
        WorkerProto.ScanResponse response;
        List<InputSplit> inputSplits = request.getScanTableInfo().getInputSplitsList().stream()
                .map(outsideItem -> new InputSplit(outsideItem.getInputInfosList().stream()
                        .map(insideItem -> InputInfo.newBuilder()
                                .setPath(insideItem.getPath())
                                .setRgStart(insideItem.getRgStart())
                                .setRgLength(insideItem.getRgLength())
                                .build())
                        .collect(Collectors.toList())) )
                .collect(Collectors.toList());

        ScanTableInfo scanTableInfo = ScanTableInfo.newBuilder()
                .setTableName(request.getScanTableInfo().getTableName())
                .setBase(request.getScanTableInfo().getBase())
                .setColumnsToRead(request.getScanTableInfo().getColumnsToReadList().toArray(new String[0]))
                .setInputSplits(inputSplits)
                .setFilter(request.getScanTableInfo().getFilter())
                .build();

        FunctionType[] functionTypes = request.getPartialAggregationInfo().getFunctionTypesList().stream()
                .map(item -> FunctionType.fromNumber(item.getNumber()))
                .toArray(FunctionType[]::new);

        PartialAggregationInfo partialAggregationInfo = PartialAggregationInfo.newBuilder()
                .setGroupKeyColumnAlias(request.getPartialAggregationInfo().getGroupKeyColumnAliasList().toArray(new String[0]))
                .setResultColumnAlias(request.getPartialAggregationInfo().getResultColumnAliasList().toArray(new String[0]))
                .setResultColumnTypes(request.getPartialAggregationInfo().getResultColumnTypesList().toArray(new String[0]))
                .setGroupKeyColumnIds(Ints.toArray(request.getPartialAggregationInfo().getGroupKeyColumnIdsList()))
                .setAggregateColumnIds(Ints.toArray(request.getPartialAggregationInfo().getAggregateColumnIdsList()))
                .setFunctionTypes(functionTypes)
                .setPartition(request.getPartialAggregationInfo().getPartition())
                .setNumPartition(request.getPartialAggregationInfo().getNumPartition())
                .build();

        StorageInfo storageInfo = StorageInfo.newBuilder()
                .
                .build();

        OutputInfo outputInfo = OutputInfo.newBuilder()
                .setPath(request.getOutput().getPath())
                .setRandomFileName(request.getOutput().getRandomFileName())
                .setStorageInfo()
                .setEncoding()
                .build();

        ScanInput input = ScanInput.newBuilder()
                .setQueryId(request.getQueryId())
                .setScanTableInfo(scanTableInfo)
                .setScanProjection(Booleans.toArray(request.getScanProjectionList()))
                .setPartialAggregationPresent(request.getPartialAggregationPresent())
                .setPartialAggregationInfo(partialAggregationInfo)
                .setOutput()
                .build();

        ScanOutput output = this.handler.handleRequest(input);

//        super.scan(request, responseObserver);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void hello(WorkerProto.HelloRequest request, StreamObserver<WorkerProto.HelloResponse> responseObserver) {
        WorkerProto.HelloResponse response;

        String output = "Hello, " + request.getName();
        response = WorkerProto.HelloResponse.newBuilder().setOutput(output).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


}
