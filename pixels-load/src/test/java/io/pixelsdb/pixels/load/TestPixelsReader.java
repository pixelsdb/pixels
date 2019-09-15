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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.load;

import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * @author hank
 */
public class TestPixelsReader
{
    String filePath = "hdfs://dbiir10:9000/pixels/pixels/test_105/v_1_order/20190111212837_0.pxl";

    @Test
    public void testPixelsReader() {
        Configuration conf = new Configuration();
        Path currentPath = new Path(filePath);
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setFS(fs)
                    .setPath(currentPath)
                    .build();

            TypeDescription schema = reader.getFileSchema();
            List<String> fieldNames = schema.getFieldNames();
            String[] cols = new String[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                cols[i] = fieldNames.get(i);
            }

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            PixelsRecordReader recordReader = reader.read(option);
            System.out.println(recordReader.getRowNumber());
            System.out.println(reader.getRowGroupInfo(0).getNumberOfRows());
            int batchSize = 10000;
            VectorizedRowBatch rowBatch;
            int len = 0;
            int num = 0;
            int row = 0;
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                row++;
                String result = rowBatch.toString();
                len += result.length();
                System.out.println("loop:" + row + "," + rowBatch.size);
                if (rowBatch.endOfFile) {
                    num += rowBatch.size;
                    break;
                }
                num += rowBatch.size;
            }
            System.out.println(row + "," + num);
            reader.close();
        } catch (IOException e) {
            System.out.println("Err path: " + currentPath.toString());
            e.printStackTrace();
        }
    }

    String orcPath = "hdfs://dbiir10:9000/pixels/pixels/test_105/v_0_order_orc/20181226133514_0.orc";

    @Test
    public void testOrcReader() {
        Configuration conf = new Configuration();
        Reader reader = null;
        try {
            reader = OrcFile.createReader(new Path(orcPath),
                    OrcFile.readerOptions(conf));
            System.out.println("Row: " + reader.getNumberOfRows());
            RecordReader rows = null;
            rows = reader.rows();
            org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            long num = 0;
            int row = 0;
            while (rows.nextBatch(batch)) {
                System.out.println("loop:" + row++ + "," + batch.size);
                if (row == 147)
                    System.out.println(batch.toString());
                num += batch.size;
            }
            System.out.println(row + "," + num);
            rows.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
