package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PbTableDao implements PbDao<MetadataProto.Table>
{
    public PbTableDao() {}

    private static final DBUtil db = DBUtil.Instance();
    private static Logger log = LogManager.getLogger(PbTableDao.class);

    @Override
    public MetadataProto.Table getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_NAME, TBL_TYPE, DBS_DB_ID FROM TBLS WHERE TBL_ID=" + id);
            if (rs.next())
            {
                MetadataProto.Table table = MetadataProto.Table.newBuilder()
                .setId(id)
                .setName(rs.getString("TBL_NAME"))
                .setType(rs.getString("TBL_TYPE"))
                .setSchemaId(rs.getLong("DBS_DB_ID")).build();
                return table;
            }
        } catch (SQLException e)
        {
            log.error("getById in TableDao", e);
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
            ResultSet rs = st.executeQuery("SELECT TBL_ID, TBL_TYPE FROM TBLS WHERE TBL_NAME='" + name +
                    "' AND DBS_DB_ID=" + schema.getId());
            if (rs.next())
            {
                MetadataProto.Table table = MetadataProto.Table.newBuilder()
                .setId(rs.getLong("TBL_ID"))
                .setName(name)
                .setType(rs.getString("TBL_TYPE"))
                .setSchemaId(schema.getId()).build();
                return table;
            }

        } catch (SQLException e)
        {
            log.error("getByNameAndDB in TableDao", e);
        }

        return null;
    }

    public List<MetadataProto.Table> getByName(String name)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_ID, TBL_TYPE, DBS_DB_ID FROM TBLS WHERE TBL_NAME='" + name + "'");
            List<MetadataProto.Table> tables = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Table table = MetadataProto.Table.newBuilder()
                .setId(rs.getLong("TBL_ID"))
                .setName(name)
                .setType(rs.getString("TBL_TYPE"))
                .setSchemaId(rs.getLong("DBS_DB_ID")).build();
                tables.add(table);
            }
            return tables;

        } catch (SQLException e)
        {
            log.error("getByName in TableDao", e);
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
            ResultSet rs = st.executeQuery("SELECT TBL_ID, TBL_NAME, TBL_TYPE, DBS_DB_ID FROM TBLS WHERE DBS_DB_ID=" + schema.getId());
            List<MetadataProto.Table> tables = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Table table = MetadataProto.Table.newBuilder()
                .setId(rs.getLong("TBL_ID"))
                .setName(rs.getString("TBL_NAME"))
                .setType(rs.getString("TBL_TYPE"))
                .setSchemaId(schema.getId()).build();
                tables.add(table);
            }
            return tables;

        } catch (SQLException e)
        {
            log.error("getBySchema in TableDao", e);
        }

        return null;
    }

    public boolean save (MetadataProto.Table table)
    {
        if (exists(table))
        {
            return update(table);
        }
        else
        {
            return insert(table);
        }
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
            log.error("exists in TableDao", e);
        }

        return false;
    }

    public boolean insert (MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO TBLS(" +
                "`TBL_NAME`," +
                "`TBL_TYPE`," +
                "`DBS_DB_ID`) VALUES (?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, table.getName());
            pst.setString(2, table.getType());
            pst.setLong(3, table.getSchemaId());
            int flag = pst.executeUpdate();
            return flag > 0;
        } catch (SQLException e)
        {
            log.error("insert in TableDao", e);
        }
        return false;
    }

    public boolean update (MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE TBLS\n" +
                "SET\n" +
                "`TBL_NAME` = ?," +
                "`TBL_TYPE` = ?\n" +
                "WHERE `TBL_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, table.getName());
            pst.setString(2, table.getType());
            pst.setLong(3, table.getId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in TableDao", e);
        }
        return false;
    }

    /**
     * We use cascade delete and cascade update in the metadata database.
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
            log.error("delete in TableDao", e);
        }
        return false;
    }
}
