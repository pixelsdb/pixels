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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.hive;

import org.junit.Test;

/**
 * Created at: 19-6-30
 * Author: hank
 */
public class DBTableNameTest
{
    @Test
    public void test ()
    {
        String dirs = "/pixels/db/table/v_0_order";
        dirs = dirs.split(",")[0];
        if (dirs.startsWith("hdfs://"))
        {
            dirs = dirs.substring(7);
        } else if(dirs.startsWith("file://"))
        {
            dirs = dirs.substring(6);
        }
        dirs = dirs.substring(dirs.indexOf('/'));
        System.out.println(dirs);
        String[] tokens = dirs.split("/");
        String[] res = new String[2];
        res[0] = tokens[2];
        res[1] = tokens[3];
        for(String s : res)
        {
            System.out.println(s);
        }

        String str = "pixels.test_105";
        tokens = str.split("\\.");
        for (String s : tokens)
        {
            System.out.println(s);
        }

    }
}
