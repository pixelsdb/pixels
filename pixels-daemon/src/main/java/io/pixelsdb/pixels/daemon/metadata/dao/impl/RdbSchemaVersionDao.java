/*
 * Copyright 2023 PixelsDB.
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

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.ColumnDao;
import io.pixelsdb.pixels.daemon.metadata.dao.DaoFactory;
import io.pixelsdb.pixels.daemon.metadata.dao.SchemaVersionDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

/**
 * @author hank
 * @create 2023-06-11
 */
public class RdbSchemaVersionDao extends SchemaVersionDao
{
    private static final Logger log = LogManager.getLogger(RdbSchemaVersionDao.class);

    public RdbSchemaVersionDao() {}

    private static final MetaDBUtil db = MetaDBUtil.Instance();
    private static final ColumnDao columnDao = DaoFactory.Instance().getColumnDao();

    @Override
    public MetadataProto.SchemaVersion getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM SCHEMA_VERSIONS WHERE SV_ID=" + id);
            if (rs.next())
            {
                Columns columns = JSON.parseObject(rs.getString("SV_COLUMNS"), Columns.class);
                MetadataProto.SchemaVersion schemaVersion = MetadataProto.SchemaVersion.newBuilder()
                        .setId(id)
                        .addAllColumns(columnDao.getAllByIds(columns.getColumnIds(), false))
                        .setTransTs(rs.getLong("SV_TRANS_TS"))
                        .setTableId(rs.getLong("TBLS_TBL_ID"))
                        // Issue #437: range index id is set to 0 if it is null in metadata.
                        .setRangeIndexId(rs.getLong("RANGE_INDEXES_RI_ID"))
                        .build();
                return schemaVersion;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbSchemaVersionDao", e);
        }

        return null;
    }

    @Override
    public long insert(MetadataProto.SchemaVersion schemaVersion)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO SCHEMA_VERSIONS(" +
                "`SV_COLUMNS`," +
                "`SV_TRANS_TS`," +
                "`TBLS_TBL_ID`," +
                "`RANGE_INDEXES_RI_ID`) VALUES (?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, JSON.toJSONString(new Columns(schemaVersion.getColumnsList())));
            pst.setLong(2, schemaVersion.getTransTs());
            pst.setLong(3, schemaVersion.getTableId());
            if (schemaVersion.hasRangeIndexId())
            {
                pst.setLong(4, schemaVersion.getRangeIndexId());
            }
            else
            {
                pst.setNull(4, Types.BIGINT);
            }
            if (pst.executeUpdate() == 1)
            {
                ResultSet rs = pst.executeQuery("SELECT LAST_INSERT_ID()");
                if (rs.next())
                {
                    return rs.getLong(1);
                }
                else
                {
                    return -1;
                }
            }
            else
            {
                return -1;
            }
        } catch (SQLException e)
        {
            log.error("insert in RdbSchemaVersionDao", e);
        }

        return -1;
    }
}
