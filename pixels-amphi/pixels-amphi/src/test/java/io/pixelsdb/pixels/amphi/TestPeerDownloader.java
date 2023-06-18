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
package io.pixelsdb.pixels.amphi;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.compactor.CompactLayout;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.apache.avro.generic.GenericArray;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class TestPeerDownloader
{
    /* Given the PATH, we can retrieve all pxl uri from s3. */
    @Test
    public void testS3Data()
            throws MetadataException, IOException, InterruptedException
    {
        String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

        // get layout
        MetadataService metadataService = new MetadataService(hostAddr, 18888);
        List<Layout> layouts = metadataService.getLayouts("tpch_1g", "lineitem");
        System.out.println("existing number of layouts: " + layouts.size());
        System.out.println("layout: " + layouts.get(0));

        // get schema
        List<Column> columns = metadataService.getColumns("tpch_1g", "lineitem", false);
        for (Column column : columns)
        {
            System.out.println(column);
        }

        // get input file paths
        Storage storage = StorageFactory.Instance().getStorage("s3");
        List<Status> statuses = storage.listStatus("s3://pixels-tpch1g/region/v-0-order");
        System.out.println(statuses.size());
        for (Status status : statuses)
        {
            System.out.println(status.getPath());
        }

        metadataService.shutdown();
    }

    /* Read pxl from s3 and retrieve the columns needed */
    @Test
    public void testPixelsReader()
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"r_name", "r_regionkey"};
        String filePath = "pixels-tpch1g/region/v-0-order/20230529201511_20.pxl";
        int batchSize = 10000;
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;
        try (PixelsReader pixelsReader = getReader(filePath);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            while (true)
            {
                rowBatch = recordReader.readBatch(batchSize);
                BinaryColumnVector r_name = (BinaryColumnVector) rowBatch.cols[0];
                LongColumnVector r_regionkey = (LongColumnVector) rowBatch.cols[1];
                if (rowBatch.endOfFile)
                {
                    for (int i = 0; i < rowBatch.size; i++) {
                        String actualStr = new String(r_name.vector[i], r_name.start[i], r_name.lens[i]);
                        System.out.println(r_regionkey.vector[i]);
                        System.out.println(actualStr);
                    }
                    break;
                }
                for (int i = 0; i < rowBatch.size; i++) {
                    String actualStr = new String(r_name.vector[i], r_name.start[i], r_name.lens[i]);
                    System.out.println(r_regionkey.vector[i]);
                    System.out.println(actualStr);
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private PixelsReader getReader(String filePath)
    {
        PixelsReader pixelsReader = null;
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("s3");
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(filePath)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return pixelsReader;
    }

    /* Simply write a parquet file based on avro schema */
    @Test
    public void testParquetWriter() throws IOException
    {
        String avroSchema = "{"
                + "\"type\":\"record\","
                + "\"name\":\"Region\","
                + "\"fields\":["
                    + "{\"name\":\"r_regionkey\",\"type\":\"int\"},"
                    + "{\"name\":\"r_name\",\"type\":\"string\"},"
                    + "{\"name\":\"r_comment\",\"type\":\"string\"}"
                + "]"
                + "}";

        Schema schema = new Schema.Parser().parse(avroSchema);

        File outputFile = new File("region.parquet");
        Path outputPath = new Path(outputFile.getPath());

        List<Integer> regionKeys = Arrays.asList(0, 1, 2, 3, 4);
        List<String> names = Arrays.asList("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST");
        List<String> comments = Arrays.asList("comment1", "comment2", "comment3", "comment4", "comment5");

        ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(outputPath)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(schema)
                .withConf(new Configuration())
                .build();

        // TODO: avro is row-based model, any column store model makes it faster?
        for (int i = 0; i < regionKeys.size(); i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("r_regionkey", regionKeys.get(i));
            record.put("r_name", names.get(i));
            record.put("r_comment", comments.get(i));
            writer.write(record);
        }

        writer.close();
    }

    /* Download two columns of region table */
    @Test
    public void testPeerDownloaderSimple() throws IOException, MetadataException
    {
        String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";
        MetadataService metadataService = new MetadataService(hostAddr, 18888);

        List<Column> columns = metadataService.getColumns("tpch_1g", "lineitem", false);
        List<Column> partial = columns.stream()
                .filter(column -> column.getName().equals("l_orderkey") || column.getName().equals("l_shipdate") || column.getName().equals("l_quantity"))
                .collect(Collectors.toList());

        String avroSchema = "{"
                + "\"type\":\"record\","
                + "\"name\":\"Lineitem\","
                + "\"fields\":["
                + "{\"name\":\"l_quantity\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":15,\"scale\":2}},"
                + "{\"name\":\"l_orderkey\",\"type\":\"long\"},"
                + "{\"name\":\"l_shipdate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}"
                + "]"
                + "}";
        Schema regionSchema = new Schema.Parser().parse(avroSchema);

        Storage inputStorage = StorageFactory.Instance().getStorage("s3");
        List<Status> statuses = inputStorage.listStatus("s3://pixels-tpch1g/lineitem/v-0-order");
        String[] sourcePaths = statuses.stream().map(status -> status.getPath()).toArray(String[]::new);

//        List<Column> columns = metadataService.getColumns("tpch_1g", "orders", false);
//        List<Column> partial = columns.stream()
//                .filter(column -> column.getName().equals("o_orderstatus") || column.getName().equals("o_orderkey"))
//                .collect(Collectors.toList());
//
//        String avroSchema = "{"
//                + "\"type\":\"record\","
//                + "\"name\":\"order\","
//                + "\"fields\":["
//                + "{\"name\":\"o_orderstatus\",\"type\":\"string\"},"
//                + "{\"name\":\"o_orderkey\",\"type\":\"long\"}"
//                + "]"
//                + "}";
//        Schema regionSchema = new Schema.Parser().parse(avroSchema);
//
//        Storage inputStorage = StorageFactory.Instance().getStorage("s3");
//        List<Status> statuses = inputStorage.listStatus("s3://pixels-tpch1g/orders/v-0-order");
//        String[] sourcePaths = statuses.stream().map(status -> status.getPath()).toArray(String[]::new);

        Storage outputStorage = StorageFactory.Instance().getStorage("file");

        PeerDownloader downloader = PeerDownloader.newBuilder()
                .setSchema(regionSchema)
                .setColumns(partial)
                .setSourcePaths(Arrays.asList(sourcePaths))
                .setInputStorage(inputStorage)
                .setOutputStorage(outputStorage)
                .setOutDirPath("/Users/backfire/test-lineitem")
                .build();

        downloader.writeParquetFile();
    }

    @Test
    public void testLoadAvroSchema()
    {
        try {
            // 加载Schema
            InputStream schemaStream = TestPeerDownloader.class.getClassLoader().getResourceAsStream("schema/tpch/lineitem.avsc");
            Schema schema = new Schema.Parser().parse(schemaStream);
            System.out.println(schema);

            // 验证Schema
            if (schema.getType() == Schema.Type.RECORD && schema.getName().equals("lineitem")) {
                System.out.println("Schema is valid.");
            } else {
                System.out.println("Schema is invalid.");
            }
        } catch (IOException e) {
            System.out.println("Failed to load schema: " + e.getMessage());
        }

    }
}
