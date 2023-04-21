package io.pixelsdb.pixels.worker.vhive;

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import io.grpc.stub.StreamObserver;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.Output;
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
    public void scan(WorkerProto.ScanInput request, StreamObserver<WorkerProto.ScanOutput> responseObserver) {
        ScanOutput output = this.scanWorker.handleRequest(protoToPhysical(request));

        WorkerProto.ScanOutput response = physicalToProto(output);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void hello(WorkerProto.HelloRequest request, StreamObserver<WorkerProto.HelloResponse> responseObserver) {
        String output = "Hello, " + request.getName();

        WorkerProto.HelloResponse response = WorkerProto.HelloResponse.newBuilder().setOutput(output).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private ScanInput protoToPhysical(WorkerProto.ScanInput request) {
        return ScanInput.newBuilder()
                .setQueryId(request.getQueryId())
                .setScanTableInfo(protoToPhysical(request.getScanTableInfo()))
                .setScanProjection(Booleans.toArray(request.getScanProjectionList()))
                .setPartialAggregationPresent(request.getPartialAggregationPresent())
                .setPartialAggregationInfo(protoToPhysical(request.getPartialAggregationInfo()))
                .setOutput(protoToPhysical(request.getOutput()))
                .build();
    }

    private ScanTableInfo protoToPhysical(WorkerProto.ScanTableInfo request) {
        return ScanTableInfo.newBuilder()
                .setTableName(request.getTableName())
                .setBase(request.getBase())
                .setColumnsToRead(request.getColumnsToReadList().toArray(new String[0]))
                .setInputSplits(
                        request.getInputSplitsList().stream()
                        .map(this::protoToPhysical)
                        .collect(Collectors.toList())
                )
                .setFilter(request.getFilter())
                .build();
    }

    private InputSplit protoToPhysical(WorkerProto.InputSplit request) {
        return new InputSplit(request.getInputInfosList().stream()
                .map(this::protoToPhysical)
                .collect(Collectors.toList()));
    }

    private InputInfo protoToPhysical(WorkerProto.InputInfo request) {
        return InputInfo.newBuilder()
                .setPath(request.getPath())
                .setRgStart(request.getRgStart())
                .setRgLength(request.getRgLength())
                .build();
    }

    private PartialAggregationInfo protoToPhysical(WorkerProto.PartialAggregationInfo request) {
        return PartialAggregationInfo.newBuilder()
                .setGroupKeyColumnAlias(request.getGroupKeyColumnAliasList().toArray(new String[0]))
                .setResultColumnAlias(request.getResultColumnAliasList().toArray(new String[0]))
                .setResultColumnTypes(request.getResultColumnTypesList().toArray(new String[0]))
                .setGroupKeyColumnIds(Ints.toArray(request.getGroupKeyColumnIdsList()))
                .setAggregateColumnIds(Ints.toArray(request.getAggregateColumnIdsList()))
                .setFunctionTypes(
                        request.getFunctionTypesList().stream()
                        .map(this::protoToPhysical)
                        .toArray(FunctionType[]::new)
                )
                .setPartition(request.getPartition())
                .setNumPartition(request.getNumPartition())
                .build();
    }

    private FunctionType protoToPhysical(WorkerProto.FunctionType request) {
        return FunctionType.fromNumber(request.getNumber());
    }

    private OutputInfo protoToPhysical(WorkerProto.OutputInfo request) {
        return OutputInfo.newBuilder()
                .setPath(request.getPath())
                .setRandomFileName(request.getRandomFileName())
                .setStorageInfo(protoToPhysical(request.getStorageInfo()))
                .setEncoding(request.getEncoding())
                .build();
    }

    private StorageInfo protoToPhysical(WorkerProto.StorageInfo request) {
        return StorageInfo.newBuilder()
                .setScheme(protoToPhysical(request.getScheme()))
                .setEndpoint(request.getEndpoint())
                .setAccessKey(request.getAccessKey())
                .setSecretKey(request.getSecretKey())
                .build();
    }

    private Storage.Scheme protoToPhysical(WorkerProto.Scheme request) {
        return Storage.Scheme.fromNumber(request.getNumber());
    }

    private WorkerProto.ScanOutput physicalToProto(ScanOutput response) {
        return WorkerProto.ScanOutput.newBuilder()
                .setBase(physicalToProto((Output)response))
                .addAllOutputs(response.getOutputs())
                .addAllRowGroupNums(response.getRowGroupNums())
                .build();
    }

    private WorkerProto.Output physicalToProto(Output response) {
        return WorkerProto.Output.newBuilder()
                .setRequestId(response.getRequestId())
                .setSuccessful(response.isSuccessful())
                .setErrorMessage(response.getErrorMessage())
                .setStartTimeMs(response.getStartTimeMs())
                .setDurationMs(response.getDurationMs())
                .setMemoryMB(response.getMemoryMB())
                .setCumulativeInputCostMs(response.getCumulativeInputCostMs())
                .setCumulativeComputeCostMs(response.getCumulativeComputeCostMs())
                .setCumulativeOutputCostMs(response.getCumulativeOutputCostMs())
                .setNumReadRequests(response.getNumReadRequests())
                .setNumWriteRequests(response.getNumWriteRequests())
                .setTotalReadBytes(response.getTotalReadBytes())
                .setTotalWriteBytes(response.getTotalWriteBytes())
                .build();
    }
}
