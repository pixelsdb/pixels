package io.pixelsdb.pixels.worker.spike;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import io.pixelsdb.pixels.common.turbo.WorkerType;

@JSONType
public class WorkerRequest {
    @JSONField(name = "workerType")

    private WorkerType workerType;
    @JSONField(name = "workerPayload")

    private String workerPayload;

    public WorkerRequest() {
    }

    public WorkerRequest(WorkerType workerType, String workerPayload) {
        this.workerType = workerType;
        this.workerPayload = workerPayload;
    }
    public String getWorkerPayload() {
        return workerPayload;
    }
    public void setWorkerPayload(String workerPayload) {
        this.workerPayload = workerPayload;
    }
    public WorkerType getWorkerType() {
        return workerType;
    }
    public void setWorkerType(WorkerType workerType) {
        this.workerType = workerType;
    }
}
