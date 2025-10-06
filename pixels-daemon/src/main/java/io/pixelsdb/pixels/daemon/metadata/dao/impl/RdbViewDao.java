/*
 * Copyright 2022 PixelsDB.
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
import io.pixelsdb.pixels.daemon.metadata.dao.ViewDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 */
public class RdbViewDao extends ViewDao
{
    private static final Logger log = LogManager.getLogger(RdbViewDao.class);

    public RdbViewDao() {}

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.View getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT VIEW_NAME, VIEW_TYPE, VIEW_DATA, DBS_DB_ID " +
                    "FROM VIEWS WHERE VIEW_ID=" + id);
            if (rs.next())
            {
                MetadataProto.View view = MetadataProto.View.newBuilder()
                .setId(id)
                .setName(rs.getString("VIEW_NAME"))
                .setType(rs.getString("VIEW_TYPE"))
                .setData(rs.getString("VIEW_DATA"))
                .setSchemaId(rs.getLong("DBS_DB_ID")).build();
                return view;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbViewDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.View> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public MetadataProto.View getByNameAndSchema (String name, MetadataProto.Schema schema)
    {
        if(schema == null)
        {
            return null;
        }
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT VIEW_ID, VIEW_TYPE, VIEW_DATA FROM VIEWS WHERE VIEW_NAME='" +
                    name + "' AND DBS_DB_ID=" + schema.getId());
            if (rs.next())
            {
                MetadataProto.View view = MetadataProto.View.newBuilder()
                .setId(rs.getLong("VIEW_ID"))
                .setName(name)
                .setType(rs.getString("VIEW_TYPE"))
                .setData(rs.getString("VIEW_DATA"))
                .setSchemaId(schema.getId()).build();
                return view;
            }
        } catch (SQLException e)
        {
            log.error("getByNameAndSchema in RdbViewDao", e);
        }

        return null;
    }

    public List<MetadataProto.View> getByName(String name)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT VIEW_ID, VIEW_TYPE, VIEW_DATA, DBS_DB_ID " +
                    "FROM VIEWS WHERE VIEW_NAME='" + name + "'");
            List<MetadataProto.View> views = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.View view = MetadataProto.View.newBuilder()
                .setId(rs.getLong("VIEW_ID"))
                .setName(name)
                .setType(rs.getString("VIEW_TYPE"))
                .setData(rs.getString("VIEW_DATA"))
                .setSchemaId(rs.getLong("DBS_DB_ID")).build();
                views.add(view);
            }
            return views;
        } catch (SQLException e)
        {
            log.error("getByName in RdbViewDao", e);
        }

        return null;
    }

    public List<MetadataProto.View> getBySchema(MetadataProto.Schema schema)
    {
        if(schema == null)
        {
            return null;
        }
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT VIEW_ID, VIEW_NAME, VIEW_TYPE, VIEW_DATA, DBS_DB_ID " +
                    "FROM VIEWS WHERE DBS_DB_ID=" + schema.getId());
            List<MetadataProto.View> views = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.View view = MetadataProto.View.newBuilder()
                .setId(rs.getLong("VIEW_ID"))
                .setName(rs.getString("VIEW_NAME"))
                .setType(rs.getString("VIEW_TYPE"))
                .setData(rs.getString("VIEW_DATA"))
                .setSchemaId(schema.getId()).build();
                views.add(view);
            }
            return views;
        } catch (SQLException e)
        {
            log.error("getBySchema in RdbViewDao", e);
        }

        return null;
    }

    public boolean exists (MetadataProto.View view)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM VIEWS WHERE VIEW_ID=" + view.getId()
                    + " OR (DBS_DB_ID=" + view.getSchemaId() +
                    " AND VIEW_NAME='" + view.getName() + "')";
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbViewDao", e);
        }

        return false;
    }

    public boolean insert (MetadataProto.View view)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO VIEWS(" +
                "`VIEW_NAME`," +
                "`VIEW_TYPE`," +
                "`VIEW_DATA`," +
                "`DBS_DB_ID`) VALUES (?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, view.getName());
            pst.setString(2, view.getType());
            pst.setString(3, view.getData());
            pst.setLong(4, view.getSchemaId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbViewDao", e);
        }

        return false;
    }

    public boolean update (MetadataProto.View view)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE VIEWS\n" +
                "SET\n" +
                "`VIEW_NAME` = ?," +
                "`VIEW_TYPE` = ?," +
                "`VIEW_DATA` = ?\n" +
                "WHERE `VIEW_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, view.getName());
            pst.setString(2, view.getType());
            pst.setString(3, view.getData());
            pst.setLong(4, view.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbViewDao", e);
        }

        return false;
    }

    public boolean deleteByNameAndSchema (String name, MetadataProto.Schema schema)
    {
        assert name !=null && schema != null;
        Connection conn = db.getConnection();
        String sql = "DELETE FROM VIEWS WHERE VIEW_NAME=? AND DBS_DB_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, name);
            pst.setLong(2, schema.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteByNameAndSchema in RdbViewDao", e);
        }

        return false;
    }
}
