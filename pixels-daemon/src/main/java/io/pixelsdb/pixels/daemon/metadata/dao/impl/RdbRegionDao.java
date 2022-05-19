package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.common.utils.DBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.RegionDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RdbRegionDao extends RegionDao {
    public RdbRegionDao() {}

    private static Logger log = LogManager.getLogger(RdbLayoutDao.class);

    private static final DBUtil db = DBUtil.Instance();

    @Override
    public MetadataProto.Region getById(long id) {
        return null;
    }

    @Override
    public List<MetadataProto.Region> getByTable(MetadataProto.Table table) {
        return null;
    }
}
