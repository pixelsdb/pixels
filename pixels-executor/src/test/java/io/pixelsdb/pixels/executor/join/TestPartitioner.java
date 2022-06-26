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
package io.pixelsdb.pixels.executor.join;

import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author hank
 * @date 26/06/2022
 */
public class TestPartitioner
{
    @Test
    public void test() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("/home/hank/Desktop/20220313083127_0.compact.pxl")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.queryId(123456);
        option.includeCols(new String[] {"c_custkey", "c_name", "c_address",
                "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"});
        option.rgRange(4, 4);
        PixelsRecordReader recordReader = pixelsReader.read(option);

        int rowBatchSize = 10000;
        int numPartition = 5;
        TypeDescription rowBatchSchema = recordReader.getResultSchema();
        VectorizedRowBatch rowBatch;
        List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult = new ArrayList<>(numPartition);
        for (int i = 0; i < numPartition; ++i)
        {
            partitionResult.add(new ConcurrentLinkedQueue<>());
        }
        Partitioner partitioner = new Partitioner(numPartition, rowBatchSize,
                rowBatchSchema, new int[] {0});

        Map<Long, String> custKeyToAddress = new HashMap<>();
        int num = 0, pnum = 0;
        do
        {
            rowBatch = recordReader.readBatch(rowBatchSize);
            if (rowBatch.size > 0)
            {
                num += rowBatch.size;
                LongColumnVector vector0 = (LongColumnVector) rowBatch.cols[0];
                BinaryColumnVector vector2 = (BinaryColumnVector) rowBatch.cols[2];
                for (int i = 0; i < rowBatch.size; ++i)
                {
                    if (!vector2.isNull[i])
                    {
                        custKeyToAddress.put(vector0.vector[i],
                                new String(vector2.vector[i], vector2.start[i], vector2.lens[i]));
                    }
                }
                Map<Integer, VectorizedRowBatch> result = partitioner.partition(rowBatch);
                if (!result.isEmpty())
                {
                    for (Map.Entry<Integer, VectorizedRowBatch> entry : result.entrySet())
                    {
                        pnum += entry.getValue().size;
                        partitionResult.get(entry.getKey()).add(entry.getValue());
                    }
                }
            }
        } while (!rowBatch.endOfFile);

        pixelsReader.close();

        VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
        for (int hash = 0; hash < tailBatches.length; ++hash)
        {
            if (!tailBatches[hash].isEmpty())
            {
                pnum += tailBatches[hash].size;
                partitionResult.get(hash).add(tailBatches[hash]);
            }
        }

        if (num != pnum)
        {
            System.out.println("error");
            throw new InvalidArgumentException();
        }

        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder().setStorage(storage)
                .setPath("/home/hank/Desktop/part-0").setPartitioned(true)
                .setEncoding(true).setPixelStride(10000).setOverwrite(true)
                .setPartKeyColumnIds(Arrays.asList(0))
                .setRowGroupSize(268435456).setSchema(rowBatchSchema).build();

        for (int hash = 0; hash < numPartition; ++hash)
        {
            ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitionResult.get(hash);
            if (!batches.isEmpty())
            {
                for (VectorizedRowBatch batch : batches)
                {
                    LongColumnVector vector0 = (LongColumnVector) batch.cols[0];
                    BinaryColumnVector vector2 = (BinaryColumnVector) batch.cols[2];
                    for (int i = 0; i < batch.size; ++i)
                    {
                        String address = new String(vector2.vector[i], vector2.start[i], vector2.lens[i]);
                        if (!custKeyToAddress.get(vector0.vector[i]).equals(address))
                        {
                            System.out.println("error");
                            throw new RuntimeException("not match");
                        }
                    }
                    pixelsWriter.addRowBatch(batch, hash);
                }
            }
        }

        pixelsWriter.close();

        for (int hash = 0; hash < numPartition; ++hash)
        {
            ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitionResult.get(hash);
            if (!batches.isEmpty())
            {
                for (VectorizedRowBatch batch : batches)
                {
                    LongColumnVector vector0 = (LongColumnVector) batch.cols[0];
                    BinaryColumnVector vector2 = (BinaryColumnVector) batch.cols[2];
                    for (int i = 0; i < batch.size; ++i)
                    {
                        String address = new String(vector2.vector[i], vector2.start[i], vector2.lens[i]);
                        if (!custKeyToAddress.get(vector0.vector[i]).equals(address))
                        {
                            System.out.println("error");
                            throw new RuntimeException("not match");
                        }
                    }
                }
            }
        }

        PixelsReader pixelsReader1 = PixelsReaderImpl.newBuilder()
                .setPath("/home/hank/Desktop/part-0")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option1 = new PixelsReaderOption();
        option1.queryId(123456);
        option1.includeCols(new String[] {"c_custkey", "c_name", "c_address",
                "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"});
        option1.rgRange(0, -1);
        PixelsRecordReader recordReader1 = pixelsReader1.read(option1);

        pnum = 0;
        do
        {
            rowBatch = recordReader1.readBatch(rowBatchSize);
            if (rowBatch.size > 0)
            {
                pnum += rowBatch.size;
                LongColumnVector vector0 = (LongColumnVector) rowBatch.cols[0];
                BinaryColumnVector vector2 = (BinaryColumnVector) rowBatch.cols[2];
                for (int i = 0; i < rowBatch.size; ++i)
                {
                    String address = new String(vector2.vector[i], vector2.start[i], vector2.lens[i]);
                    if (!custKeyToAddress.get(vector0.vector[i]).equals(address))
                    {
                        System.out.println(custKeyToAddress.get(vector0.vector[i]) + " != " + address);
                        System.out.println(pnum);
                        throw new RuntimeException("not match");
                    }
                }
            }
        } while (!rowBatch.endOfFile);

        pixelsReader1.close();
        if (num != pnum)
        {
            System.out.println("error");
            throw new RuntimeException("...");
        }
    }

    @Test
    public void test2() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setPath("/home/hank/Desktop/20220313083127_0.compact.pxl")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.queryId(123456);
        option.includeCols(new String[] {"c_custkey", "c_name", "c_address",
                "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"});
        option.rgRange(4, 4);
        PixelsRecordReader recordReader = pixelsReader.read(option);

        int rowBatchSize = 10000;
        int numPartition = 5;
        TypeDescription rowBatchSchema = recordReader.getResultSchema();
        VectorizedRowBatch rowBatch;
        List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult = new ArrayList<>(numPartition);
        for (int i = 0; i < numPartition; ++i)
        {
            partitionResult.add(new ConcurrentLinkedQueue<>());
        }
        Partitioner partitioner = new Partitioner(numPartition, rowBatchSize,
                rowBatchSchema, new int[] {0});

        Map<Long, Long> custKeyToNationKey = new HashMap<>();
        int num = 0, pnum = 0;
        do
        {
            rowBatch = recordReader.readBatch(rowBatchSize);
            if (rowBatch.size > 0)
            {
                num += rowBatch.size;
                LongColumnVector vector0 = (LongColumnVector) rowBatch.cols[0];
                LongColumnVector vector3 = (LongColumnVector) rowBatch.cols[3];
                for (int i = 0; i < rowBatch.size; ++i)
                {
                    if (!vector3.isNull[i])
                    {
                        custKeyToNationKey.put(vector0.vector[i], vector3.vector[i]);
                    }
                }
                Map<Integer, VectorizedRowBatch> result = partitioner.partition(rowBatch);
                if (!result.isEmpty())
                {
                    for (Map.Entry<Integer, VectorizedRowBatch> entry : result.entrySet())
                    {
                        pnum += entry.getValue().size;
                        partitionResult.get(entry.getKey()).add(entry.getValue());
                    }
                }
            }
        } while (!rowBatch.endOfFile);

        pixelsReader.close();

        VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
        for (int hash = 0; hash < tailBatches.length; ++hash)
        {
            if (!tailBatches[hash].isEmpty())
            {
                pnum += tailBatches[hash].size;
                partitionResult.get(hash).add(tailBatches[hash]);
            }
        }

        if (num != pnum)
        {
            System.out.println("error");
            throw new InvalidArgumentException();
        }

        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder().setStorage(storage)
                .setPath("/home/hank/Desktop/part-0").setPartitioned(true)
                .setEncoding(true).setPixelStride(10000).setOverwrite(true)
                .setPartKeyColumnIds(Arrays.asList(0))
                .setRowGroupSize(268435456).setSchema(rowBatchSchema).build();

        for (int hash = 0; hash < numPartition; ++hash)
        {
            ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitionResult.get(hash);
            if (!batches.isEmpty())
            {
                for (VectorizedRowBatch batch : batches)
                {
                    LongColumnVector vector0 = (LongColumnVector) batch.cols[0];
                    LongColumnVector vector3 = (LongColumnVector) batch.cols[3];
                    for (int i = 0; i < batch.size; ++i)
                    {
                        if (custKeyToNationKey.get(vector0.vector[i]) != vector3.vector[i])
                        {
                            System.out.println("error");
                            throw new RuntimeException("not match");
                        }
                    }
                    pixelsWriter.addRowBatch(batch, hash);
                }
            }
        }

        pixelsWriter.close();

        for (int hash = 0; hash < numPartition; ++hash)
        {
            ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitionResult.get(hash);
            if (!batches.isEmpty())
            {
                for (VectorizedRowBatch batch : batches)
                {
                    LongColumnVector vector0 = (LongColumnVector) batch.cols[0];
                    LongColumnVector vector3 = (LongColumnVector) batch.cols[3];
                    for (int i = 0; i < batch.size; ++i)
                    {
                        if (custKeyToNationKey.get(vector0.vector[i]) != vector3.vector[i])
                        {
                            System.out.println("error");
                            throw new RuntimeException("not match");
                        }
                    }
                }
            }
        }

        PixelsReader pixelsReader1 = PixelsReaderImpl.newBuilder()
                .setPath("/home/hank/Desktop/part-0")
                .setStorage(storage).setPixelsFooterCache(new PixelsFooterCache()).setEnableCache(false).build();
        PixelsReaderOption option1 = new PixelsReaderOption();
        option1.queryId(123456);
        option1.includeCols(new String[] {"c_custkey", "c_name", "c_address",
                "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"});
        option1.rgRange(0, -1);
        PixelsRecordReader recordReader1 = pixelsReader1.read(option1);

        pnum = 0;
        do
        {
            rowBatch = recordReader1.readBatch(rowBatchSize);
            if (rowBatch.size > 0)
            {
                pnum += rowBatch.size;
                LongColumnVector vector0 = (LongColumnVector) rowBatch.cols[0];
                LongColumnVector vector3 = (LongColumnVector) rowBatch.cols[3];
                for (int i = 0; i < rowBatch.size; ++i)
                {
                    if (custKeyToNationKey.get(vector0.vector[i]) != vector3.vector[i])
                    {
                        System.out.println("error");
                        throw new RuntimeException("not match");
                    }
                }
            }
        } while (!rowBatch.endOfFile);

        pixelsReader1.close();
        if (num != pnum)
        {
            System.out.println("error");
            throw new RuntimeException("...");
        }
    }
}
