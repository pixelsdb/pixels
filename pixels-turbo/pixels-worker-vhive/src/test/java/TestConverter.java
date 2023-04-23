import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.NonPartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class TestConverter {

    private TableInfo genTableInfo() {
        return new TableInfo("mytable", true, new String[]{"col1", "col2"});
    }

    @Test
    public void testTableInfo() {
        Converter<TableInfo> converter = new Converter<>(TableInfo.class);
        converter.executeTest(genTableInfo());
    }

    private InputInfo genInputInfo() {
        return new InputInfo("mypath", 0, 100);
    }

    @Test
    public void testInputInfo() {
        Converter<InputInfo> converter = new Converter<>(InputInfo.class);
        converter.executeTest(genInputInfo());
    }

    private InputSplit genInputSplit() {
        InputInfo info1 = new InputInfo("mypath1", 0, 100);
        InputInfo info2 = new InputInfo("mypath2", 100, 200);
        return new InputSplit(Arrays.asList(info1, info2));
    }

    @Test
    public void testInputSplit() {
        Converter<InputSplit> converter = new Converter<>(InputSplit.class);
        converter.executeTest(genInputSplit());
    }

    private ScanTableInfo genScanTableInfo() {
        return new ScanTableInfo("mytable", true, Collections.singletonList(genInputSplit()), new String[]{"col1", "col2", "col3"}, "predicates");
    }

    @Test
    public void testScanTableInfo() {
        Converter<ScanTableInfo> converter = new Converter<>(ScanTableInfo.class);
        converter.executeTest(genScanTableInfo());
    }

    @Test
    public void testFunctionType() {
        Converter<FunctionType> converter = new Converter<>(FunctionType.class);
        converter.executeTest(FunctionType.UNKNOWN);
        converter.executeTest(FunctionType.SUM);
        converter.executeTest(FunctionType.MIN);
        converter.executeTest(FunctionType.MAX);
    }

    private PartialAggregationInfo genPartialAggregationInfo() {
        return new PartialAggregationInfo(
                new String[]{"alias1", "alias2"},
                new String[]{"alias3", "alias4", "alias5"},
                new String[]{"Integer", "Time", "Text"},
                new int[]{1, 2},
                new int[]{1, 10, 100, 1000},
                new FunctionType[]{FunctionType.UNKNOWN, FunctionType.SUM, FunctionType.MAX},
                true,
                10
        );
    }

    @Test
    public void testPartialAggregationInfo() {
        Converter<PartialAggregationInfo> converter = new Converter<>(PartialAggregationInfo.class);
        converter.executeTest(genPartialAggregationInfo());
    }

    @Test
    public void testScheme() {
        Converter<Storage.Scheme> converter = new Converter<>(Storage.Scheme.class);
        converter.executeTest(Storage.Scheme.hdfs);
        converter.executeTest(Storage.Scheme.file);
        converter.executeTest(Storage.Scheme.s3);
        converter.executeTest(Storage.Scheme.minio);
        converter.executeTest(Storage.Scheme.redis);
        converter.executeTest(Storage.Scheme.gcs);
        converter.executeTest(Storage.Scheme.mock);
    }

    private StorageInfo genStorageInfo() {
        return new StorageInfo(Storage.Scheme.gcs, "endpoint", "accesskey", "secretkey");
    }

    @Test
    public void testStorageInfo() {
        Converter<StorageInfo> converter = new Converter<>(StorageInfo.class);
        converter.executeTest(genStorageInfo());
    }

    private OutputInfo genOutputInfo() {
        return new OutputInfo(
                "mypath",
                true,
                genStorageInfo(),
                false
        );
    }

    @Test
    public void testOutputInfo() {
        Converter<OutputInfo> converter = new Converter<>(OutputInfo.class);
        converter.executeTest(genOutputInfo());
    }

    private Output genOutput() {
        return new Output(
                "myid",
                true,
                "",
                0,
                100,
                1024,
                100,
                200,
                300,
                2,
                1,
                2048,
                4096
        );
    }

    @Test
    public void testOutput() {
        Converter<Output> converter = new Converter<>(Output.class);
        converter.executeTest(genOutput());
    }

    private NonPartitionOutput genNonPartitionOutput() {
        return new NonPartitionOutput(genOutput(), Arrays.asList("output1", "output2"), Arrays.asList(100, 200));
    }

    @Test
    public void testNonPartitionOutput() {
        Converter<NonPartitionOutput> converter = new Converter<>(NonPartitionOutput.class);
        converter.executeTest(genNonPartitionOutput());
    }

    private ScanInput genScanInput() {
        return new ScanInput(
                100,
                genScanTableInfo(),
                new boolean[]{true, false, true},
                true,
                genPartialAggregationInfo(),
                genOutputInfo()
        );
    }

    @Test
    public void testScanInput() {
        Converter<ScanInput> converter = new Converter<>(ScanInput.class);
        converter.executeTest(genScanInput());
    }

    private ScanOutput genScanOutput() {
        return new ScanOutput(
                genNonPartitionOutput()
        );
    }

    @Test
    public void testScanOutput() {
        Converter<ScanOutput> converter = new Converter<>(ScanOutput.class);
        converter.executeTest(genScanOutput());
    }

}
