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
package io.pixelsdb.pixels.index.rockset;

import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexFactory;
import org.junit.Test;

public class TestRocksetIndex
{
    @Test
    public void test() throws SinglePointIndexException
    {
        RocksetIndex rocksetIndex = (RocksetIndex) SinglePointIndexFactory.Instance().
                getSinglePointIndex(new SinglePointIndexFactory.TableIndex(
                        1L, 1L, SinglePointIndex.Scheme.rockset, true));

        long dbHandle = 0;

        try
        {
            // 1. get dbHandle
            dbHandle = rocksetIndex.getDbHandle();
            System.out.println("DB handle: " + dbHandle);

            // Prepare test keys/values
            byte[] key1 = "key1".getBytes();
            byte[] val1 = "val1".getBytes();
            byte[] key2 = "key2".getBytes();
            byte[] val2 = "val2".getBytes();

            // 2. DB basic ops
            System.out.println("=== Basic DB ops ===");
            rocksetIndex.DBput(dbHandle, key1, val1);
            rocksetIndex.DBput(dbHandle, key2, val2);

            byte[] got1 = rocksetIndex.DBget(dbHandle, key1);
            System.out.println("Retrieved key1: " + (got1 == null ? "null" : new String(got1)));

            rocksetIndex.DBdelete(dbHandle, key1);
            byte[] deleted = rocksetIndex.DBget(dbHandle, key1);
            System.out.println("After delete, key1: " + (deleted == null ? "null" : new String(deleted)));

            // 3. Iterator test
            System.out.println("=== Iterator ops ===");
            long it = rocksetIndex.DBNewIterator(dbHandle);
            byte[] seekKey = "zzzz".getBytes(); // seek for last key <= "zzzz"
            rocksetIndex.IteratorSeekForPrev(it, seekKey);

            while (rocksetIndex.IteratorIsValid(it))
            {
                byte[] ikey = rocksetIndex.IteratorKey(it);
                byte[] ival = rocksetIndex.IteratorValue(it);
                System.out.println("Iter kv: " + new String(ikey) + " -> " + new String(ival));
                rocksetIndex.IteratorPrev(it);
            }
            rocksetIndex.IteratorClose(it);

            // 4. WriteBatch test
            System.out.println("=== WriteBatch ops ===");
            long wb = rocksetIndex.WriteBatchCreate();

            byte[] key3 = "key3".getBytes();
            byte[] val3 = "val3".getBytes();
            byte[] key4 = "key4".getBytes();
            byte[] val4 = "val4".getBytes();

            rocksetIndex.WriteBatchPut(wb, key3, val3);
            rocksetIndex.WriteBatchPut(wb, key4, val4);

            rocksetIndex.DBWrite(dbHandle, wb);

            byte[] got3 = rocksetIndex.DBget(dbHandle, key3);
            byte[] got4 = rocksetIndex.DBget(dbHandle, key4);
            System.out.println("Retrieved key3: " + new String(got3));
            System.out.println("Retrieved key4: " + new String(got4));

            // Delete via batch
            rocksetIndex.WriteBatchClear(wb);
            rocksetIndex.WriteBatchDelete(wb, key3);
            rocksetIndex.DBWrite(dbHandle, wb);

            byte[] deleted3 = rocksetIndex.DBget(dbHandle, key3);
            System.out.println("After batch delete, key3: " + (deleted3 == null ? "null" : new String(deleted3)));

            // cleanup batch
            rocksetIndex.WriteBatchDestroy(wb);
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