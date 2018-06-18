package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import cn.edu.ruc.iir.pixels.common.metadata.Schema;
import org.apache.commons.logging.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
}
