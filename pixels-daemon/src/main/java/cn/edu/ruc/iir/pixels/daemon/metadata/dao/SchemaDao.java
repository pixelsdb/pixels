package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import org.apache.commons.logging.Log;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SchemaDao implements Dao<Schema>
{
    public SchemaDao() {}

    private static final DBUtil db = DBUtil.Instance();
    private static final Log log = LogFactory.Instance().getLog();

    @Override
    public Schema getById(int id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT DB_NAME, DB_DESC FROM DBS WHERE DB_ID=" + id);
            if (rs.next())
            {
                Schema schema = new Schema();
                schema.setId(id);
                schema.setName(rs.getString("DB_NAME"));
                schema.setDesc(rs.getString("DB_DESC"));
                return schema;
            }

        } catch (SQLException e)
        {
            log.error("getById in SchemaDao", e);
        }

        return null;
    }

    @Override
    public List<Schema> getAll()
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM DBS");
            List<Schema> schemas = new ArrayList<>();
            while (rs.next())
            {
                Schema schema = new Schema();
                schema.setId(rs.getInt("DB_ID"));
                schema.setName(rs.getString("DB_NAME"));
                schema.setDesc(rs.getString("DB_DESC"));
                schemas.add(schema);
            }
            return schemas;

        } catch (SQLException e)
        {
            log.error("getAll in SchemaDao", e);
        }

        return null;
    }

    public Schema getByName(String name)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT DB_ID, DB_DESC FROM DBS WHERE DB_NAME='" + name + "'");
            if (rs.next())
            {
                Schema schema = new Schema();
                schema.setId(rs.getInt("DB_ID"));
                schema.setName(name);
                schema.setDesc(rs.getString("DB_DESC"));
                return schema;
            }

        } catch (SQLException e)
        {
            log.error("getByName in SchemaDao", e);
        }

        return null;
    }

    public boolean save (Schema schema)
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

    public boolean exists (Schema schema)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT 1 FROM DBS WHERE DB_ID=" + schema.getId());
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

    private boolean insert (Schema schema)
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

    private boolean update (Schema schema)
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
            pst.setInt(3, schema.getId());
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
        String sql = "DELETE FROM DBS DB_NAME=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, name);
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("deleteByName in SchemaDao", e);
        }
        return false;
    }
}
