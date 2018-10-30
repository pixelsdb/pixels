package cn.edu.ruc.iir.pixels.load.util;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.common.utils.FileUtil;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.TableElement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.load.util
 * @ClassName: LoaderUtil
 * @Description:
 * @author: tao
 * @date: Create in 2018-10-30 13:30
 **/
public class LoaderUtil {

    public static boolean executeDDL(SqlParser parser, String dbName, String schemaPath)
            throws SQLException
    {
        String sql = FileUtil.readFileToString(schemaPath);
        if (sql == null)
        {
            return false;
        }
        CreateTable createTable = (CreateTable) parser.createStatement(sql, new ParsingOptions());
        String tableName = createTable.getName().toString();

        Table table = new Table();
        SchemaDao schemaDao = new SchemaDao();
        Schema schema = schemaDao.getByName(dbName);
        table.setId(-1);
        table.setName(tableName);
        table.setType("");
        table.setSchema(schema);
        TableDao tableDao = new TableDao();
        boolean flag = tableDao.save(table);
        // ODOT-------------------------------------------

        // COLS
        if (flag) {
            // TBLS
            int tableID = tableDao.getByName(tableName).get(0).getId();
            // ODOT-------------------------------------------
            List<TableElement> elements = createTable.getElements();
            int size = elements.size();
            addColumnsByTableID(elements, size, tableID);
            return true;
        } else {
            return false;
        }
    }

    private static void addColumnsByTableID(List<TableElement> elements, int size, int tableID)
            throws SQLException
    {
        String prefix = "INSERT INTO COLS (COL_NAME, COL_TYPE, TBLS_TBL_ID) VALUES (?, ?, ?)";
        DBUtil instance = DBUtil.Instance();
        Connection conn = instance.getConnection();
        conn.setAutoCommit(false);
        PreparedStatement pst = conn.prepareStatement(prefix);
        for (int i = 0; i < size; i++) {
            ColumnDefinition column = (ColumnDefinition) elements.get(i);
            String name = column.getName().toString();
            String type = column.getType();
            pst.setString(1, name);
            pst.setString(2, type);
            pst.setInt(3, tableID);
            pst.addBatch();
            pst.executeBatch();
            conn.commit();
        }
        pst.close();
        conn.close();
    }
}
