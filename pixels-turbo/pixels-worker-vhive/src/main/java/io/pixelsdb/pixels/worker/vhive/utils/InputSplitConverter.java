package io.pixelsdb.pixels.worker.vhive.utils;

import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.worker.vhive.WorkerProto;

import java.util.stream.Collectors;

public class InputSplitConverter implements Converter<InputSplit, WorkerProto.InputSplit> {
    public static final InputSplitConverter INSTANCE = new InputSplitConverter();

    private InputSplitConverter() {}


    @Override
    public WorkerProto.InputSplit physicalToProto(InputSplit request) {
        return WorkerProto.InputSplit.newBuilder()
                .addAllInputInfos(request.getInputInfos().stream()
                    .map(InputInfoConverter.INSTANCE::physicalToProto)
                    .collect(Collectors.toList()))
                .build();
    }

    @Override
    public InputSplit protoToPhysical(WorkerProto.InputSplit request) {
        return new InputSplit();
    }
}
