/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.common.layout;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import org.junit.Test;

/**
 * @author hank
 * @date 14/07/2022
 */
public class TestCostBasedSplitsIndex
{
    @Test
    public void test() throws MetadataException
    {
        MetadataService metadataService = MetadataService.Instance();

        CostBasedSplitsIndex index = new CostBasedSplitsIndex(0L, 0L, metadataService,
                new SchemaTableName("tpch", "orders"),4, 8);

        ColumnSet columnSet = new ColumnSet();
        columnSet.addColumn("o_orderkey");
        columnSet.addColumn("o_custkey");
        columnSet.addColumn("o_orderdate");
        columnSet.addColumn("o_shippriority");

        SplitPattern splitPattern = index.search(columnSet);
        System.out.println(splitPattern.getSplitSize());
    }
}
