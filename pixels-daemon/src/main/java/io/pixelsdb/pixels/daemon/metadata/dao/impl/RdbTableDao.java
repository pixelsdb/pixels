/*
 * Copyright 2019 PixelsDB.
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

import io.pixelsdb.pixels.common.utils.DBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.TableDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 */
public class RdbTableDao extends TableDao
{
    public RdbTableDao() {}

    private static final DBUtil db = DBUtil.Instance();
    private static Logger log = LogManager.getLogger(RdbTableDao.class);

    @Override
    public MetadataProto.Table getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_NAME, TBL_TYPE, TBL_STORAGE_SCHEME, DBS_DB_ID " +
                    "FROM TBLS WHERE TBL_ID=" + id);
            if (rs.next())
            {
                MetadataProto.Table table = MetadataProto.Table.newBuilder()
                .setId(id)
                .setName(rs.getString("TBL_NAME"))
                .setType(rs.getString("TBL_TYPE"))
                .setStorageScheme(rs.getString("TBL_STORAGE_SCHEME"))
                .setSchemaId(rs.getLong("DBS_DB_ID")).build();
                return table;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbTableDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.Table> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public MetadataProto.Table getByNameAndSchema (String name, MetadataProto.Schema schema)
    {
        if(schema == null)
        {
            return null;
        }
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_ID, TBL_TYPE, TBL_STORAGE_SCHEME FROM TBLS WHERE TBL_NAME='" +
                    name + "' AND DBS_DB_ID=" + schema.getId());
            if (rs.next())
            {
                MetadataProto.Table table = MetadataProto.Table.newBuilder()
                .setId(rs.getLong("TBL_ID"))
                .setName(name)
                .setType(rs.getString("TBL_TYPE"))
                .setStorageScheme(rs.getString("TBL_STORAGE_SCHEME"))
                .setSchemaId(schema.getId()).build();
                return table;
            }

        } catch (SQLException e)
        {
            log.error("getByNameAndDB in RdbTableDao", e);
        }

        return null;
    }

    public List<MetadataProto.Table> getByName(String name)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_ID, TBL_TYPE, TBL_STORAGE_SCHEME, DBS_DB_ID " +
                    "FROM TBLS WHERE TBL_NAME='" + name + "'");
            List<MetadataProto.Table> tables = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Table table = MetadataProto.Table.newBuilder()
                .setId(rs.getLong("TBL_ID"))
                .setName(name)
                .setType(rs.getString("TBL_TYPE"))
                .setStorageScheme(rs.getString("TBL_STORAGE_SCHEME"))
                .setSchemaId(rs.getLong("DBS_DB_ID")).build();
                tables.add(table);
            }
            return tables;

        } catch (SQLException e)
        {
            log.error("getByName in RdbTableDao", e);
        }

        return null;
    }

    public List<MetadataProto.Table> getBySchema(MetadataProto.Schema schema)
    {
        if(schema == null)
        {
            return null;
        }
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_ID, TBL_NAME, TBL_TYPE, TBL_STORAGE_SCHEME, DBS_DB_ID " +
                    "FROM TBLS WHERE DBS_DB_ID=" + schema.getId());
            List<MetadataProto.Table> tables = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Table table = MetadataProto.Table.newBuilder()
                .setId(rs.getLong("TBL_ID"))
                .setName(rs.getString("TBL_NAME"))
                .setType(rs.getString("TBL_TYPE"))
                .setStorageScheme(rs.getString("TBL_STORAGE_SCHEME"))
                .setSchemaId(schema.getId()).build();
                tables.add(table);
            }
            return tables;

        } catch (SQLException e)
        {
            log.error("getBySchema in RdbTableDao", e);
        }

        return null;
    }

    /**
     * If the table with the same id or with the same db_id and table name exists,
     * this method returns false.
     * @param table
     * @return
     */
    public boolean exists (MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM TBLS WHERE TBL_ID=" + table.getId()
                    + " OR (DBS_DB_ID=" + table.getSchemaId() +
                    " AND TBL_NAME='" + table.getName() + "')";
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbTableDao", e);
        }

        return false;
    }

    public boolean insert (MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO TBLS(" +
                "`TBL_NAME`," +
                "`TBL_TYPE`," +
                "`TBL_STORAGE_SCHEME`," +
                "`DBS_DB_ID`) VALUES (?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, table.getName());
            pst.setString(2, table.getType());
            pst.setString(3, table.getStorageScheme());
            pst.setLong(4, table.getSchemaId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbTableDao", e);
        }
        return false;
    }

    public boolean update (MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE TBLS\n" +
                "SET\n" +
                "`TBL_NAME` = ?," +
                "`TBL_TYPE` = ?," +
                "`TBL_STORAGE_SCHEME` = ?\n" +
                "WHERE `TBL_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, table.getName());
            pst.setString(2, table.getType());
            pst.setString(3, table.getStorageScheme());
            pst.setLong(4, table.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbTableDao", e);
        }
        return false;
    }

    /**
     * We ensure cascade delete and update in the metadata database.
     * If you delete a table by this method, all the layouts and columns of the table
     * will be deleted.
     * @param name
     * @param schema
     * @return
     */
    public boolean deleteByNameAndSchema (String name, MetadataProto.Schema schema)
    {
        assert name !=null && schema != null;
        Connection conn = db.getConnection();
        String sql = "DELETE FROM TBLS WHERE TBL_NAME=? AND DBS_DB_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, name);
            pst.setLong(2, schema.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("delete in RdbTableDao", e);
        }
        return false;
    }
}
