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

import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.SchemaDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 */
public class RdbSchemaDao extends SchemaDao
{
    public RdbSchemaDao() {}

    private static final MetaDBUtil db = MetaDBUtil.Instance();
    private static final Logger log = LogManager.getLogger(RdbSchemaDao.class);

    @Override
    public MetadataProto.Schema getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT DB_NAME, DB_DESC FROM DBS WHERE DB_ID=" + id);
            if (rs.next())
            {
                MetadataProto.Schema schema = MetadataProto.Schema.newBuilder()
                .setId(id)
                .setName(rs.getString("DB_NAME"))
                .setDesc(rs.getString("DB_DESC")).build();
                return schema;
            }

        } catch (SQLException e)
        {
            log.error("getById in RdbSchemaDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.Schema> getAll()
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM DBS");
            List<MetadataProto.Schema> schemas = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Schema schema = MetadataProto.Schema.newBuilder()
                .setId(rs.getLong("DB_ID"))
                .setName(rs.getString("DB_NAME"))
                .setDesc(rs.getString("DB_DESC")).build();
                schemas.add(schema);
            }
            return schemas;

        } catch (SQLException e)
        {
            log.error("getAll in RdbSchemaDao", e);
        }

        return null;
    }

    public MetadataProto.Schema getByName(String name)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT DB_ID, DB_DESC FROM DBS WHERE DB_NAME='" + name + "'");
            if (rs.next())
            {
                MetadataProto.Schema schema = MetadataProto.Schema.newBuilder()
                .setId(rs.getLong("DB_ID"))
                .setName(name)
                .setDesc(rs.getString("DB_DESC")).build();
                return schema;
            }

        } catch (SQLException e)
        {
            log.error("getByName in RdbSchemaDao", e);
        }

        return null;
    }

    public boolean exists (MetadataProto.Schema schema)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT 1 FROM DBS WHERE DB_ID=" + schema.getId() +
            " OR DB_NAME='" + schema.getName() + "'");
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbSchemaDao", e);
        }

        return false;
    }

    public boolean insert (MetadataProto.Schema schema)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO DBS(" +
                "`DB_NAME`," +
                "`DB_DESC`) VALUES (?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, schema.getName());
            pst.setString(2, schema.getDesc());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbSchemaDao", e);
        }

        return false;
    }

    public boolean update (MetadataProto.Schema schema)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE DBS\n" +
                "SET\n" +
                "`DB_NAME` = ?," +
                "`DB_DESC` = ?\n" +
                "WHERE `DB_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, schema.getName());
            pst.setString(2, schema.getDesc());
            pst.setLong(3, schema.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbSchemaDao", e);
        }

        return false;
    }

    /**
     * We ensure cascade delete and update in the metadata database.
     * If you delete a schema by this method, all the tables, layouts and columns of the schema
     * will be deleted.
     * @param name
     * @return
     */
    public boolean deleteByName (String name)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM DBS WHERE DB_NAME=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, name);
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteByName in RdbSchemaDao", e);
        }

        return false;
    }
}
