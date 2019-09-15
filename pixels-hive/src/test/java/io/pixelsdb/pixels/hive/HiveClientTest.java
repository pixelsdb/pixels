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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.hive;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveClientTest {

    @Test
    public void SelectTest() {
        String hostUrl = "jdbc:hive2://presto00:10000/default";
        String userName = "iir";
        String passWord = "";
        HiveClient client = HiveClient.Instance(hostUrl, userName, passWord);

        String statement = "select Count(*) from pixelssd";
        ResultSet res = client.select(statement);
        try {
            while (res.next()) {
                System.out.println(res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void IsTableExistTest() {
        HiveClient client = HiveClient.Instance("jdbc:hive2://presto00:10000/default", "presto", "");
        int count = client.IsTableExist("select count(*) from text");
        System.out.println(count);
    }

}
