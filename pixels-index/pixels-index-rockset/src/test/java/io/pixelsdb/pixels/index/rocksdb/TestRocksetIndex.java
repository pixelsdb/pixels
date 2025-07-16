/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.index.rocksdb;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexFactory;
import org.junit.Test;

public class TestRocksetIndex
{
    @Test
    public void test() throws SinglePointIndexException
    {
        RocksetIndex rocksetIndex = (RocksetIndex) SinglePointIndexFactory.Instance()
                .getSinglePointIndex(1L, 1L, SinglePointIndex.Scheme.rockset);
        long dbHandle = 0;

        try
        {
            // 1. get dbHandle
            dbHandle = rocksetIndex.getDbHandle();
            System.out.println("DB handle: " + dbHandle);

            // 2. test write
            byte[] testKey = "test_key".getBytes();
            byte[] testValue = "test_value".getBytes();

            System.out.println("Putting key-value pair...");
            rocksetIndex.DBput(dbHandle, testKey, testValue);

            // 3. test read
            System.out.println("Getting value...");
            byte[] retrievedValue = rocksetIndex.DBget(dbHandle, testKey);
            if (retrievedValue != null) 
            {
                System.out.println("Retrieved value: " + new String(retrievedValue));
            } 
            else 
            {
                System.out.println("Key not found");
            }

            // 4. test delete
            System.out.println("Deleting key...");
            rocksetIndex.DBdelete(dbHandle, testKey);
            byte[] deletedValue = rocksetIndex.DBget(dbHandle, testKey);
            if (deletedValue == null) 
            {
                System.out.println("Key successfully deleted");
            }
        }
        finally
        {
            // 5. confirm close
            if (dbHandle != 0) 
            {
                System.out.println("Closing DB...");
                rocksetIndex.CloseDB(dbHandle);
            }
        }
    }
}