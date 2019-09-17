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
package io.pixelsdb.pixels.test;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * pixels
 *
 * @author guodong
 */
public class CopyMetadata
{
    @Test
    public void copy()
    {
        ConfigFactory config = ConfigFactory.Instance();
        String driver = config.getProperty("metadata.db.driver");

        String srcURL = "jdbc:mysql://dbiir27:3306/pixels_metadata?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
        String srcUser = "pixels";
        String srcPwd = "pixels27";

        String dstURL = "jdbc:mysql://dbiir01:3306/pixels_metadata?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
        String dstUser = "pixels";
        String dstPwd = "pixels";

        int srcTableId = 3;

        try
        {
            Class.forName(driver);
            Connection srcConn = DriverManager.getConnection(srcURL, srcUser, srcPwd);
            Connection dstConn = DriverManager.getConnection(dstURL, dstUser, dstPwd);
            Statement srcConnStatement = srcConn.createStatement();
            Statement dstConnStatement = dstConn.createStatement();

            // get table from src
            ResultSet resultSet = srcConnStatement.executeQuery("SELECT TBL_NAME, TBL_TYPE, DBS_DB_ID FROM TBLS WHERE TBL_ID=" + srcTableId);
            String tblName = "";
            String tblType = "";
            int dbId = 0;
            while (resultSet.next())
            {
                tblName = resultSet.getString("TBL_NAME");
                tblType = resultSet.getString("TBL_TYPE");
                dbId = resultSet.getInt("DBS_DB_ID");
            }
            resultSet.close();
            srcConnStatement.close();

            // get schema from src
            srcConnStatement = srcConn.createStatement();
            resultSet = srcConnStatement.executeQuery("SELECT DB_NAME FROM DBS WHERE DB_ID=" + dbId);
            String dbName = "";
            while (resultSet.next())
            {
                dbName = resultSet.getString("DB_NAME");
            }
            resultSet.close();
            srcConnStatement.close();

            // find schema in dst
            resultSet = dstConnStatement.executeQuery("SELECT DB_ID FROM DBS WHERE DB_NAME='" + dbName + "'");
            while (resultSet.next())
            {
                dbId = resultSet.getInt("DB_ID");
            }
            resultSet.close();
            dstConnStatement.close();

            // insert table into dst
            dstConnStatement = dstConn.createStatement();
            int ret = dstConnStatement.executeUpdate(String.format("INSERT INTO TBLS(TBL_NAME, TBL_TYPE, DBS_DB_ID) VALUES('%s', '%s', %s)", tblName, tblType, dbId));
            System.out.println("Update code " + ret);
            dstConnStatement.close();

            // get id of the newly inserted table
            int dstTableId = 0;
            dstConnStatement = dstConn.createStatement();
            resultSet = dstConnStatement.executeQuery("SELECT TBL_ID FROM TBLS WHERE TBL_NAME='" + tblName + "'");
            while (resultSet.next())
            {
                dstTableId = resultSet.getInt("TBL_ID");
            }
            resultSet.close();
            dstConnStatement.close();

            // insert cols
            srcConnStatement = srcConn.createStatement();
            dstConnStatement = dstConn.createStatement();
            resultSet = srcConnStatement.executeQuery("SELECT COL_NAME, COL_TYPE, COL_SIZE FROM COLS WHERE TBLS_TBL_ID=" + srcTableId);
            while (resultSet.next())
            {
                String colName = resultSet.getString("COL_NAME");
                String colType = resultSet.getString("COL_TYPE");
                String colSize = resultSet.getString("COL_SIZE");
                dstConnStatement.executeUpdate(String.format("INSERT INTO COLS(COL_NAME, COL_TYPE, COL_SIZE, TBLS_TBL_ID) VALUES('%s', '%s', %s, %s)", colName, colType, colSize, dstTableId));
            }
            resultSet.close();
            srcConnStatement.close();
            dstConnStatement.close();

            // copy layouts from src to dst
            srcConnStatement = srcConn.createStatement();
            dstConnStatement = dstConn.createStatement();
            resultSet = srcConnStatement.executeQuery("SELECT LAYOUT_VERSION, LAYOUT_CREATE_AT, LAYOUT_PERMISSION, " +
                    "LAYOUT_ORDER, LAYOUT_ORDER_PATH, LAYOUT_COMPACT, LAYOUT_COMPACT_PATH, " +
                    "LAYOUT_SPLITS FROM LAYOUTS WHERE LAYOUT_PERMISSION=1 AND TBLS_TBL_ID=" + srcTableId);
            while (resultSet.next())
            {
                String layout_version = resultSet.getString("LAYOUT_VERSION");
                String layout_create_at = resultSet.getString("LAYOUT_CREATE_AT");
                String layout_permission = resultSet.getString("LAYOUT_PERMISSION");
                String layout_order = resultSet.getString("LAYOUT_ORDER");
                String layout_order_path = resultSet.getString("LAYOUT_ORDER_PATH");
                String layout_compact = resultSet.getString("LAYOUT_COMPACT");
                String layout_compact_path = resultSet.getString("LAYOUT_COMPACT_PATH");
                String layout_splits = resultSet.getString("LAYOUT_SPLITS");

                if (layout_permission.equalsIgnoreCase("1"))
                {
                    dstConnStatement.executeUpdate(String.format("INSERT INTO LAYOUTS(LAYOUT_VERSION, LAYOUT_CREATE_AT, " +
                                    "LAYOUT_PERMISSION, LAYOUT_ORDER, LAYOUT_ORDER_PATH, " +
                                    "LAYOUT_COMPACT, LAYOUT_COMPACT_PATH, LAYOUT_SPLITS, TBLS_TBL_ID) " +
                                    "VALUES(%s, %s, %s, '%s', '%s', '%s', '%s', '%s', %s)",
                            layout_version, layout_create_at, layout_permission,
                            layout_order, layout_order_path, layout_compact, layout_compact_path,
                            layout_splits, dstTableId));
                }
            }
            resultSet.close();
            srcConnStatement.close();
            dstConnStatement.close();
        }
        catch (SQLException | ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }
}
