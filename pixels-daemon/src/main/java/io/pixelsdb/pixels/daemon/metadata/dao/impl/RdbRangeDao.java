/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.RangeDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @author hank
 * @create 2024-05-25
 */
public class RdbRangeDao extends RangeDao
{
    public RdbRangeDao() {}

    private static final Logger log = LogManager.getLogger(RdbRangeDao.class);

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.Range getById(long id)
    {
        return null;
    }

    @Override
    public List<MetadataProto.Range> getAllByRangeIndexId(long rangeIndexId)
    {
        return null;
    }

    @Override
    public boolean exists(MetadataProto.Range range)
    {
        return false;
    }

    @Override
    public long insert(MetadataProto.Range range)
    {
        return 0;
    }

    @Override
    public boolean update(MetadataProto.Range range)
    {
        return false;
    }

    @Override
    public boolean deleteById(long id)
    {
        return false;
    }
}
