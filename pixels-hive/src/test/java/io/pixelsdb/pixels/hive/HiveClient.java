/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.hive;

import java.sql.*;

public class HiveClient {
    private final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private Connection conn;

    private static HiveClient instance = null;

    private HiveClient(String hostUrl, String userName, String passWord) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // todo exception handler
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(hostUrl, userName, passWord);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static HiveClient Instance(String hostUrl, String userName, String passWord) {
        if (instance == null) {
            instance = new HiveClient(hostUrl, userName, passWord);
        }
        return instance;
    }

    public void execute(String statement) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(statement);
        } catch (SQLException e) {
            // todo exception handler
            e.printStackTrace();
        }
    }


    public ResultSet select(String statement) {
        ResultSet res = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            res = stmt.executeQuery(statement);
        } catch (SQLException e) {
            // todo exception handler
            e.printStackTrace();
        }
        return res;
    }

    public int IsTableExist(String table) {
        int num = -1;
        ResultSet res = null;
        try {
            res = instance.select("select count(*) from " + table);
            if (res != null)
                while (res.next()) {
                    num = Integer.valueOf(res.getString(1));
                }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;
    }

    public void drop(String table) {
        execute("DROP TABLE IF EXISTS " + table);
    }
}
