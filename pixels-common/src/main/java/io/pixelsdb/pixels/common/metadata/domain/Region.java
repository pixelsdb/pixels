package io.pixelsdb.pixels.common.metadata.domain;

import io.pixelsdb.pixels.daemon.MetadataProto;
import lombok.Getter;
import lombok.Setter;

public class Region extends Base {
    @Getter
    @Setter
    private long tableId;

    public Region() {
    }

    public Region(MetadataProto.Region region) {
        this.setId(region.getId());
        this.tableId = region.getTableId();
    }

}
