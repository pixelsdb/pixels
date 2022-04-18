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
package io.pixelsdb.pixels.core.lambda;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import io.pixelsdb.pixels.core.predicate.TableScanFilter;
import org.junit.Test;

/**
 * @author hank
 * Created at: 11/04/2022
 */
public class TestInput
{
    @Test
    public void testParseScanInput()
    {
        String json = "{\"queryId\":123456789123456789,\"inputs\":[{\"filePath\":\"pixels-tpch/orders/0.pxl\",\"rgStart\":0," +
                "\"rgLength\":4}," + "{\"filePath\":\"pixels-tpch/orders/1.pxl\",\"rgStart\":0,\"rgLength\":4}]," +
                "\"splitSize\":8," + "\"output\":{\"folder\":\"pixels-test/hank/\",\"endpoint\":\"http://hostname:9000\"," +
                "\"accessKey\":\"test\",\"secretKey\":\"password\",\"encoding\":true}," +
                "\"cols\":[\"o_orderkey\",\"o_custkey\",\"o_orderstatus\",\"o_orderdate\"]," +
                "\"filter\":\"{\\\"schemaName\\\":\\\"tpch\\\",\\\"tableName\\\":\\\"orders\\\"," +
                "\\\"columnFilters\\\":{1:{\\\"columnName\\\":\\\"o_orderkey\\\",\\\"columnType\\\":\\\"LONG\\\"," +
                "\\\"filterJson\\\":\\\"{\\\\\\\"javaType\\\\\\\":\\\\\\\"long\\\\\\\",\\\\\\\"isAll\\\\\\\":false," +
                "\\\\\\\"isNone\\\\\\\":false,\\\\\\\"allowNull\\\\\\\":false,\\\\\\\"ranges\\\\\\\":[{" +
                "\\\\\\\"lowerBound\\\\\\\":{\\\\\\\"type\\\\\\\":\\\\\\\"UNBOUNDED\\\\\\\"}," +
                "\\\\\\\"upperBound\\\\\\\":{\\\\\\\"type\\\\\\\":\\\\\\\"INCLUDED\\\\\\\",\\\\\\\"value\\\\\\\":100}}," +
                "{\\\\\\\"lowerBound\\\\\\\":{\\\\\\\"type\\\\\\\":\\\\\\\"EXCLUDED\\\\\\\",\\\\\\\"value\\\\\\\":200}," +
                "\\\\\\\"upperBound\\\\\\\":{\\\\\\\"type\\\\\\\":\\\\\\\"UNBOUNDED\\\\\\\"}}]," +
                "\\\\\\\"discreteValues\\\\\\\":[]}\\\"}}}\"}";
        Gson gson = new Gson();
        ScanInput scanInput = gson.fromJson(json, ScanInput.class);
        assert scanInput.getInputs().size() == 2;
        assert scanInput.getSplitSize() == 8;
        assert scanInput.getCols().size() == 4;
        assert scanInput.getOutput().getFolder().equals("pixels-test/hank");
        assert scanInput.getOutput().isEncoding();
        TableScanFilter filter = JSON.parseObject(scanInput.getFilter(), TableScanFilter.class);
        assert filter.getSchemaName().equals("tpch");
        assert filter.getTableName().equals("orders");
        assert filter.getColumnFilters().size() == 1;
        assert filter.getColumnFilter(0).getFilter().getRangeCount() == 2;
        assert filter.getColumnFilter(0).getFilter().getDiscreteValueCount() == 0;
    }
}
