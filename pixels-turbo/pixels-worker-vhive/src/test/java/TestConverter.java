/*
 * Copyright 2023 PixelsDB.
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
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class TestConverter
{
    private InputInfo genInputInfo()
    {
        return new InputInfo("mypath", 0, 100);
    }

    @Test
    public void testInputInfo()
    {
        Converter<InputInfo> converter = new Converter<>(InputInfo.class);
        converter.executeTest(genInputInfo());
    }

    private InputSplit genInputSplit()
    {
        InputInfo info1 = new InputInfo("mypath1", 0, 100);
        InputInfo info2 = new InputInfo("mypath2", 100, 200);
        return new InputSplit(Arrays.asList(info1, info2));
    }

    @Test
    public void testInputSplit()
    {
        Converter<InputSplit> converter = new Converter<>(InputSplit.class);
        converter.executeTest(genInputSplit());
    }

    private ScanTableInfo genScanTableInfo()
    {
        return new ScanTableInfo(
                "mytable",
                true,
                new String[]{"col1", "col2", "col3"},
                new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                Collections.singletonList(genInputSplit()),
                "predicates");
    }

    @Test
    public void testScanTableInfo()
    {
        Converter<ScanTableInfo> converter = new Converter<>(ScanTableInfo.class);
        converter.executeTest(genScanTableInfo());
    }

    @Test
    public void testFunctionType()
    {
        Converter<FunctionType> converter = new Converter<>(FunctionType.class);
        converter.executeTest(FunctionType.UNKNOWN);
        converter.executeTest(FunctionType.SUM);
        converter.executeTest(FunctionType.MIN);
        converter.executeTest(FunctionType.MAX);
    }

    private PartialAggregationInfo genPartialAggregationInfo()
    {
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
    public void testPartialAggregationInfo()
    {
        Converter<PartialAggregationInfo> converter = new Converter<>(PartialAggregationInfo.class);
        converter.executeTest(genPartialAggregationInfo());
    }

    @Test
    public void testScheme()
    {
        Converter<Storage.Scheme> converter = new Converter<>(Storage.Scheme.class);
        converter.executeTest(Storage.Scheme.hdfs);
        converter.executeTest(Storage.Scheme.file);
        converter.executeTest(Storage.Scheme.s3);
        converter.executeTest(Storage.Scheme.minio);
        converter.executeTest(Storage.Scheme.redis);
        converter.executeTest(Storage.Scheme.gcs);
        converter.executeTest(Storage.Scheme.mock);
    }

    private StorageInfo genStorageInfo()
    {
        return new StorageInfo(Storage.Scheme.gcs,
                "region", "endpoint", "accesskey", "secretkey");
    }

    @Test
    public void testStorageInfo()
    {
        Converter<StorageInfo> converter = new Converter<>(StorageInfo.class);
        converter.executeTest(genStorageInfo());
    }

    private OutputInfo genOutputInfo()
    {
        return new OutputInfo(
                "mypath",
                genStorageInfo(),
                false
        );
    }

    @Test
    public void testOutputInfo()
    {
        Converter<OutputInfo> converter = new Converter<>(OutputInfo.class);
        converter.executeTest(genOutputInfo());
    }

    private ScanInput genScanInput()
    {
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
    public void testScanInput()
    {
        Converter<ScanInput> converter = new Converter<>(ScanInput.class);
        converter.executeTest(genScanInput());
    }

}
