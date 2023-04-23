import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TestConverter {
    @Test
    public void testTableInfo() {
        TableInfo info = new TableInfo("mytable", true, new String[]{"col1", "col2"});
        String json = JSON.toJSONString(info);

        TableInfo converted = JSON.parseObject(json, TableInfo.class);
        assertEquals(converted, info);
    }

    @Test
    public void testInputInfo() {
        InputInfo info = new InputInfo("mypath", 0, 100);
        String json = JSON.toJSONString(info);

        InputInfo converted = JSON.parseObject(json, InputInfo.class);
        assertEquals(converted, info);
    }

    @Test
    public void testInputSplit() {
        InputInfo info1 = new InputInfo("mypath1", 0, 100);
        InputInfo info2 = new InputInfo("mypath2", 100, 200);
        InputSplit split = new InputSplit(Arrays.asList(info1, info2));
        String json = JSON.toJSONString(split);

        InputSplit converted = JSON.parseObject(json, InputSplit.class);
        assertEquals(converted, split);
    }

    @Test
    public void testScanTableInfo() {
        InputInfo info1 = new InputInfo("mypath1", 0, 100);
        InputInfo info2 = new InputInfo("mypath2", 100, 200);
        InputSplit split = new InputSplit(Arrays.asList(info1, info2));
        ScanTableInfo info = new ScanTableInfo("mytable", true, Collections.singletonList(split), new String[]{"col1", "col2", "col3"}, "predicates");
        String json = JSON.toJSONString(info);

        ScanTableInfo converted = JSON.parseObject(json, ScanTableInfo.class);
        assertEquals(converted, info);
    }

    @Test
    public void testFunctionType() {
        assertEquals(JSON.parseObject(JSON.toJSONString(FunctionType.UNKNOWN), FunctionType.class), FunctionType.UNKNOWN);
        assertEquals(JSON.parseObject(JSON.toJSONString(FunctionType.SUM), FunctionType.class), FunctionType.SUM);
        assertEquals(JSON.parseObject(JSON.toJSONString(FunctionType.MIN), FunctionType.class), FunctionType.MIN);
        assertEquals(JSON.parseObject(JSON.toJSONString(FunctionType.MAX), FunctionType.class), FunctionType.MAX);
    }

    @Test
    public void testPartialAggregationInfo() {
        String[] groupKeyColumnAlias = new String[]{"alias1", "alias2"};
        int[] groupKeyColumnIds = new int[]{1, 2};
        String[] resultColumnAlias = new String[]{"alias3", "alias4", "alias5"};
        String[] resultColumnTypes = new String[]{"Integer", "Time", "Text"};
        int[] aggregateColumnIds = new int[]{1, 10, 100, 1000};
        FunctionType[] functionTypes = new FunctionType[]{FunctionType.UNKNOWN, FunctionType.SUM, FunctionType.MAX};
        PartialAggregationInfo info = new PartialAggregationInfo(
                groupKeyColumnAlias,
                resultColumnAlias,
                resultColumnTypes,
                groupKeyColumnIds,
                aggregateColumnIds,
                functionTypes,
                true,
                10
        );
        String json = JSON.toJSONString(info);

        PartialAggregationInfo converted = JSON.parseObject(json, PartialAggregationInfo.class);
        assertEquals(converted, info);
    }

    @Test
    public void testScheme() {
        assertEquals(JSON.parseObject(JSON.toJSONString(Storage.Scheme.hdfs), Storage.Scheme.class), Storage.Scheme.hdfs);
        assertEquals(JSON.parseObject(JSON.toJSONString(Storage.Scheme.file), Storage.Scheme.class), Storage.Scheme.file);
        assertEquals(JSON.parseObject(JSON.toJSONString(Storage.Scheme.s3), Storage.Scheme.class), Storage.Scheme.s3);
        assertEquals(JSON.parseObject(JSON.toJSONString(Storage.Scheme.minio), Storage.Scheme.class), Storage.Scheme.minio);
        assertEquals(JSON.parseObject(JSON.toJSONString(Storage.Scheme.redis), Storage.Scheme.class), Storage.Scheme.redis);
        assertEquals(JSON.parseObject(JSON.toJSONString(Storage.Scheme.gcs), Storage.Scheme.class), Storage.Scheme.gcs);
        assertEquals(JSON.parseObject(JSON.toJSONString(Storage.Scheme.mock), Storage.Scheme.class), Storage.Scheme.mock);
    }

    @Test
    public void testStorageInfo() {
        
    }

}
