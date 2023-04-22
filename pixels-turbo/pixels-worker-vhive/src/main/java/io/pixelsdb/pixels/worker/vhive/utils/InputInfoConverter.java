package io.pixelsdb.pixels.worker.vhive.utils;

import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.worker.vhive.WorkerProto;

public class InputInfoConverter implements Converter<InputInfo, WorkerProto.InputInfo>{
    public static final InputInfoConverter INSTANCE = new InputInfoConverter();

    private InputInfoConverter() {}


    @Override
    public WorkerProto.InputInfo physicalToProto(InputInfo request) {
        return WorkerProto.InputInfo.newBuilder()
                .setPath(request.getPath())
                .setRgStart(request.getRgStart())
                .setRgLength(request.getRgLength())
                .build();
    }

    @Override
    public InputInfo protoToPhysical(WorkerProto.InputInfo request) {
        return InputInfo.newBuilder()
                .setPath(request.getPath())
                .setRgStart(request.getRgStart())
                .setRgLength(request.getRgLength())
                .build();
    }
}
