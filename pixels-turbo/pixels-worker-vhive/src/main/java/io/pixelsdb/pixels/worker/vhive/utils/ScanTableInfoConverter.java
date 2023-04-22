//package io.pixelsdb.pixels.worker.vhive.utils;
//
//import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
//import io.pixelsdb.pixels.worker.vhive.WorkerProto;
//
//import java.util.Arrays;
//import java.util.stream.Collectors;
//
//public class ScanTableInfoConverter implements Converter<ScanTableInfo, WorkerProto.ScanTableInfo> {
//    public static final ScanTableInfoConverter INSTANCE = new ScanTableInfoConverter();
//
//    private ScanTableInfoConverter() {}
//
//
//    @Override
//    public WorkerProto.ScanTableInfo physicalToProto(ScanTableInfo request) {
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
//    @Override
//    public ScanTableInfo protoToPhysical(WorkerProto.ScanTableInfo request) {
//        return ScanTableInfo.newBuilder()
//                .setTableName(request.getTableName())
//                .setBase(request.getBase())
//                .setColumnsToRead(request.getColumnsToReadList().toArray(new String[0]))
//                .setInputSplits(
//                        request.getInputSplitsList().stream()
//                                .map(this::protoToPhysical)
//                                .collect(Collectors.toList())
//                )
//                .setFilter(request.getFilter())
//                .build();
//    }
//}
