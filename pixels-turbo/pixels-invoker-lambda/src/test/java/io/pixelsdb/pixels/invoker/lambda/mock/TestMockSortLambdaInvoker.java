package io.pixelsdb.pixels.invoker.lambda.mock;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.SortInput;
import io.pixelsdb.pixels.planner.plan.physical.output.SortOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class TestMockSortLambdaInvoker
{
    @Test
    public void testOrders()
    {
        for (int i = 6191; i <= 6191; ++i)
        {
            String filter =
                    "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                            "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\"," +
                            "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                            "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                            "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                            "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                            "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                            "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                            "\\\"discreteValues\\\":[]}\"}}}";
            SortInput input = new SortInput();
            input.setTransId(123456);
            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setTableName("orders");
            String prefix = new String("/home/ubuntu/test/orders/v-0-compact/20241225084427_");
            tableInfo.setInputSplits(Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 0, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 4, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 8, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 12, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 16, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 20, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 24, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 28, 4)))));
            tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
            tableInfo.setFilter(filter);
            tableInfo.setBase(true);
            tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.file, null, null, null, null));
            input.setTableInfo(tableInfo);
            input.setProjection(new boolean[]{true, true, true, true});
            input.setKeyColumnIds(new int[]{0});
            input.setOutput(new OutputInfo("/home/ubuntu/test/pixels-lambda-test/orders_" + i,
                    new StorageInfo(Storage.Scheme.file, null, null, null, null), true));

            System.out.println(JSON.toJSONString(input));
            MockSortWorker mockSortWorker = new MockSortWorker();

            SortOutput output = mockSortWorker.process(input);
            System.out.println(output.getOutputs().size());
            System.out.println(output.getOutputs());
            System.out.println(Joiner.on(",").join(output.getOutputs()));
        }
    }

    @Test
    public void testLineitem() throws ExecutionException, InterruptedException
    {
        for (int i = 6002; i < 6004; ++i)
        {
            String filter =
                    "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
            SortInput input = new SortInput();
            input.setTransId(123456);
            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setTableName("lineitem");
            String prefix = new String("/home/ubuntu/test/lineitem/v-0-compact/20241225084317_");
            tableInfo.setInputSplits(Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 0, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 4, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 8, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 12, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 16, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 20, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 24, 4))),
                    new InputSplit(Arrays.asList(new InputInfo(prefix + i + "_compact.pxl", 28, 4)))));
            tableInfo.setFilter(filter);
            tableInfo.setBase(true);
            tableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
            tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.file, null, null, null, null));
            input.setTableInfo(tableInfo);
            input.setProjection(new boolean[]{true, true, true, true});
            input.setKeyColumnIds(new int[]{0});
            input.setOutput(new OutputInfo("/home/ubuntu/test/pixels-lambda-test/lineitem_" + i,
                    new StorageInfo(Storage.Scheme.file, null, null, null, null), true));

            System.out.println(JSON.toJSONString(input));
            MockSortWorker mockSortWorker = new MockSortWorker();
            SortOutput output = mockSortWorker.process(input);
            System.out.println(output.getOutputs().size());
            System.out.println(Joiner.on(",").join(output.getOutputs()));
        }
    }
}
