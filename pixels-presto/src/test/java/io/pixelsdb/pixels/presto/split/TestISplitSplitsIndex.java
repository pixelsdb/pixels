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
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.presto.split;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.metadata.domain.Splits;
import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.PrestoException;
import io.pixelsdb.pixels.common.layout.SplitsIndex;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestISplitSplitsIndex
{
    @Test
    public void testLocal () throws IOException
    {
        IndexName entry = new IndexName("test","t1");
        BufferedReader schemaReader = new BufferedReader(
                new FileReader(
                        "/home/hank/dev/idea-projects/pixels/pixels-presto/target/classes/105_schema.text"));
        List<String> columnOrder = new ArrayList<>();
        String line;
        while ((line = schemaReader.readLine()) != null)
        {
            columnOrder.add(line.split("\t")[0]);
        }
        schemaReader.close();
        BufferedReader workloadReader = new BufferedReader(
                new FileReader("/home/hank/dev/idea-projects/pixels/pixels-presto/target/classes/105_workload.text")
        );
        List<SplitPattern> splitPatterns = new ArrayList<>();
        int i = 0;
        while ((line = workloadReader.readLine()) != null)
        {
            String[] columns = line.split("\t")[2].split(",");
            SplitPattern splitPattern = new SplitPattern();
            for (String column : columns)
            {
                splitPattern.addColumn(column);
            }
            splitPattern.setSplitSize(i++);
            splitPatterns.add(splitPattern);
        }
        workloadReader.close();
        SplitsIndex splitsIndex = new InvertedSplitsIndex(columnOrder, splitPatterns, 16);
        IndexFactory.Instance().cacheSplitsIndex(entry, splitsIndex);
        splitsIndex = IndexFactory.Instance().getSplitsIndex(new IndexName("test", "t1"));
        ColumnSet columnSet = new ColumnSet();
        String[] columns = {"QueryDate_","Market","IsBotVNext","IsNormalQuery","Vertical","AppInfoServerName","AppInfoClientName","QueryDate_","TrafficCount"};
        for (String column : columns)
        {
            columnSet.addColumn(column);
        }
        System.out.println(splitsIndex.search(columnSet).toString());
    }

    @Test
    public void testRemote () throws MetadataException
    {
        MetadataService metadataService = new MetadataService("dbiir01", 18888);
        List<Layout> layouts = metadataService.getLayouts("pixels", "test_105");
        for (Layout layout : layouts)
        {
            // get index
            int version = layout.getVersion();
            IndexName indexName = new IndexName("pixels", "test_105");
            InvertedSplitsIndex index = (InvertedSplitsIndex) IndexFactory.Instance().getSplitsIndex(indexName);
            Order order = JSON.parseObject(layout.getOrder(), Order.class);
            Splits splits = JSON.parseObject(layout.getSplits(), Splits.class);
            if (index == null)
            {
                index = getInverted(order, splits, indexName);
            }
            else
            {
                int indexVersion = index.getVersion();
                if (indexVersion < version) {
                    index = getInverted(order, splits, indexName);
                }
            }

            /**
             * QueryDate_,
             RequestTimeUTCMinute,
             SUM(TrafficCount) AS TrafficCount
             FROM
             testnull_pixels
             WHERE
             IsBotVNext = false AND
             AppInfoServerName IN ('www.bing.com') AND
             AppInfoClientName IN ('Browser') AND
             Market IN ('zh-CN') AND
             QueryDate_ >= '2010-03-01' AND QueryDate_ <= '2018-03-01' AND
             IsHomepageView = true
             */
            // get split size
            ColumnSet columnSet = new ColumnSet();
            columnSet.addColumn("QueryDate_".toLowerCase());
            columnSet.addColumn("RequestTimeUTCMinute".toLowerCase());
            columnSet.addColumn("TrafficCount".toLowerCase());
            columnSet.addColumn("IsBotVNext".toLowerCase());
            columnSet.addColumn("AppInfoServerName".toLowerCase());
            columnSet.addColumn("AppInfoClientName".toLowerCase());
            columnSet.addColumn("Market".toLowerCase());
            columnSet.addColumn("IsHomepageView".toLowerCase());
            SplitPattern bestPattern = index.search(columnSet);
            int splitSize = bestPattern.getSplitSize();
            int rowGroupNum = splits.getNumRowGroupInBlock();
            System.out.println(bestPattern.toString());
            System.out.println(rowGroupNum);
            System.out.println(splitSize);
        }
    }

    private InvertedSplitsIndex getInverted(Order order, Splits splits, IndexName indexName) {
        List<String> columnOrder = order.getColumnOrder();
        InvertedSplitsIndex index;
        try {
            index = new InvertedSplitsIndex(columnOrder, SplitPattern.buildPatterns(columnOrder, splits), splits.getNumRowGroupInBlock());
            IndexFactory.Instance().cacheSplitsIndex(indexName, index);
        } catch (IOException e) {
            throw new PrestoException(PixelsErrorCode.PIXELS_INVERTED_INDEX_ERROR, e);
        }
        return index;
    }
}
