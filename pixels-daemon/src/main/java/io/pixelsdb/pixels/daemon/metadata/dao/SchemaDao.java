package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.common.utils.DBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SchemaDao implements Dao<MetadataProto.Schema>
{
    public SchemaDao() {}

    private static final DBUtil db = DBUtil.Instance();
    private static Logger log = LogManager.getLogger(SchemaDao.class);

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
            log.error("getById in SchemaDao", e);
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
            log.error("getAll in SchemaDao", e);
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
            log.error("getByName in SchemaDao", e);
        }

        return null;
    }

    public boolean save (MetadataProto.Schema schema)
    {
        if (exists(schema))
        {
            return update(schema);
        }
        else
        {
            return insert(schema);
        }
    }

    @SuppressWarnings("Duplicates")
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
            log.error("exists in SchemaDao", e);
        }

        return false;
    }

    @SuppressWarnings("Duplicates")
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
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in SchemaDao", e);
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
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in SchemaDao", e);
        }
        return false;
    }

    /**
     * We use cascade delete and cascade update in the metadata database.
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
            log.error("deleteByName in SchemaDao", e);
        }
        return false;
    }
}
