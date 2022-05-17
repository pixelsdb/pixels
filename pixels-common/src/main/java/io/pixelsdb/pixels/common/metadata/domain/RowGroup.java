package io.pixelsdb.pixels.common.metadata.domain;

import io.pixelsdb.pixels.daemon.MetadataProto;
import lombok.Getter;
import lombok.Setter;

public class RowGroup extends Base {
    @Getter
    @Setter
    private String filePath;
    @Getter
    @Setter
    private long layoutId;
    @Getter
    @Setter
    private long fileRgIdx;
    @Getter
    @Setter
    private boolean isWriteBuffer;

    public RowGroup(MetadataProto.RowGroup rowGroup) {
        this.setId(rowGroup.getId());
        this.filePath = rowGroup.getFilePath();
        this.layoutId = rowGroup.getLayoutId();
        this.fileRgIdx = rowGroup.getFileRgIdx();
        this.isWriteBuffer = rowGroup.getIsWriteBuffer();
    }

    public MetadataProto.RowGroup toProto() {
        return MetadataProto.RowGroup.newBuilder()
                .setId(this.getId())
                .setFilePath(this.filePath)
                .setLayoutId(this.layoutId)
                .setFileRgIdx(this.fileRgIdx)
                .setIsWriteBuffer(this.isWriteBuffer)
                .build();
    }
}
