//package io.pixelsdb.pixels.worker.vhive.utils;
//
//import com.google.common.primitives.Booleans;
//import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
//import io.pixelsdb.pixels.worker.vhive.WorkerProto;
//
//public class ScanInputConverter implements Converter<ScanInput, WorkerProto.ScanInput> {
//    public static final ScanInputConverter INSTANCE = new ScanInputConverter();
//
//    private ScanInputConverter() {}
//
//
//    @Override
//    public WorkerProto.ScanInput physicalToProto(ScanInput request) {
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
//    @Override
//    public ScanInput protoToPhysical(WorkerProto.ScanInput request) {
//        return ScanInput.newBuilder()
//                .setQueryId(request.getQueryId())
//                .setScanTableInfo(protoToPhysical(request.getScanTableInfo()))
//                .setScanProjection(Booleans.toArray(request.getScanProjectionList()))
//                .setPartialAggregationPresent(request.getPartialAggregationPresent())
//                .setPartialAggregationInfo(protoToPhysical(request.getPartialAggregationInfo()))
//                .setOutput(protoToPhysical(request.getOutput()))
//                .build();;
//    }
//}
