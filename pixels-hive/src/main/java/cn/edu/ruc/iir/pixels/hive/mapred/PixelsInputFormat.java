/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.hive.mapred;

import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Splits;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.split.*;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.LayoutDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import cn.edu.ruc.iir.pixels.hive.common.PixelsRW;
import cn.edu.ruc.iir.pixels.hive.common.PixelsSplit;
import cn.edu.ruc.iir.pixels.hive.common.PixelsStruct;
import cn.edu.ruc.iir.pixels.hive.common.SchemaTableName;
import com.alibaba.fastjson.JSON;
import com.coreos.jetcd.data.KeyValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An old mapred InputFormat for Pixels files.
 * refers to {@link org.apache.hadoop.hive.ql.io.orc.OrcInputFormat}
 *
 * <P>
 * Created at: 19-6-15
 * Author: hank
 * </P>
 */
public class PixelsInputFormat
        extends FileInputFormat<NullWritable, PixelsStruct>
{
    private static Logger log = LogManager.getLogger(PixelsInputFormat.class);

    /**
     * Get the {@link RecordReader} for the given {@link InputSplit}.
     *
     * <p>It is the responsibility of the <code>RecordReader</code> to respect
     * record boundaries while processing the logical split to present a
     * record-oriented view to the individual task.</p>
     *
     * @param inputSplit the {@link InputSplit}
     * @param conf the job that this split belongs to
     * @return a {@link RecordReader}
     */
    @Override
    public RecordReader<NullWritable, PixelsStruct>
    getRecordReader(InputSplit inputSplit,
                    JobConf conf,
                    Reporter reporter) throws IOException
    {
        PixelsSplit split;
        if (inputSplit instanceof PixelsSplit)
        {
            split = (PixelsSplit) inputSplit;
        }
        else if (inputSplit instanceof FileSplit)
        {
            split = new PixelsSplit((FileSplit) inputSplit);
        }
        else
        {
            throw new IOException("inputSplit must be PixelsSplit or FileSplit");
        }
        PixelsRW.ReaderOptions options = PixelsRW.readerOptions(conf, split);
        PixelsReader reader = PixelsRW.createReader(split.getPath(), options);
        return new PixelsMapredRecordReader(reader, options);
    }

    /**
     * Splits files returned by {@link #listStatus(JobConf)} when
     * they're too big.
     * set hive.input.format=cn.edu.ruc.iir.pixels.hive.mapred.PixelsInputFormat
     * in hive to use this method.
     *
     * @param job
     * @param numSplits
     */
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
    {
        StopWatch sw = new StopWatch().start();

        Path[] inputPaths = getInputPaths(job);
        assert inputPaths != null && inputPaths.length > 0;
        FSFactory fsFactory = new FSFactory(FileSystem.get(job));
        SchemaTableName st = getSchemaTableName(job);
        String[] includedColumns = ColumnProjectionUtils.getReadColumnNames(job);
        ConfigFactory config = ConfigFactory.Instance();
        boolean cacheEnabled = Boolean.parseBoolean(config.getProperty("cache.enabled"));
        int fixedSplitSize = Integer.parseInt(config.getProperty("fixed.split.size"));

        List<Layout> layouts = getLayouts(st);

        numSplits = numSplits == 0 ? 1 : numSplits;
        // generate splits
        ArrayList<PixelsSplit> pixelsSplits = new ArrayList<>(numSplits);

        for (Layout layout : layouts)
        {
            // get index
            int version = layout.getVersion();
            IndexEntry indexEntry = new IndexEntry(st.getSchemaName(), st.getTableName());

            Order order = JSON.parseObject(layout.getOrder(), Order.class);
            Splits splits = JSON.parseObject(layout.getSplits(), Splits.class);

            // get split size
            int splitSize;
            if (fixedSplitSize > 0)
            {
                splitSize = fixedSplitSize;
            }
            else
            {
                ColumnSet columnSet = new ColumnSet();
                for (String columnName : includedColumns)
                {
                    columnSet.addColumn(columnName);
                }

                Inverted index = (Inverted) IndexFactory.Instance().getIndex(indexEntry);
                if (index == null)
                {
                    log.debug("split index not exist in factory, building index...");
                    index = getInverted(order, splits, indexEntry);
                }
                else
                {
                    int indexVersion = index.getVersion();
                    if (indexVersion < version) {
                        log.debug("split index is expired, building new index...");
                        index = getInverted(order, splits, indexEntry);
                    }
                }

                AccessPattern bestPattern = index.search(columnSet);
                splitSize = bestPattern.getSplitSize();
            }
            log.error("using split size: " + splitSize);
            int rowGroupNum = splits.getNumRowGroupInBlock();

            if(cacheEnabled)
            {
                Compact compact = layout.getCompactObject();
                int cacheBorder = compact.getCacheBorder();
                List<String> cacheColumnletOrders = compact.getColumnletOrder().subList(0, cacheBorder);
                String cacheVersion;
                EtcdUtil etcdUtil = EtcdUtil.Instance();
                KeyValue keyValue = etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL);
                if(keyValue != null)
                {
                    // 1. get version
                    cacheVersion = keyValue.getValue().toStringUtf8();
                    log.debug("cache version: " + cacheVersion);
                    // 2. get files of each node
                    List<KeyValue> nodeFiles = etcdUtil.getKeyValuesByPrefix(Constants.CACHE_LOCATION_LITERAL + cacheVersion);
                    if(nodeFiles.size() > 0)
                    {
                        Map<String, String> fileLocations = new HashMap<>();
                        for (KeyValue kv : nodeFiles)
                        {
                            String[] files = kv.getValue().toStringUtf8().split(";");
                            String node = kv.getKey().toStringUtf8().split("_")[2];
                            for(String file : files)
                            {
                                fileLocations.put(file, node);
                            }
                        }
                        try
                        {
                            // 3. add splits in orderedPath
                            List<Path> orderedPaths = fsFactory.listFiles(layout.getOrderPath());
                            for (Path path : orderedPaths)
                            {
                                long fileLength = fsFactory.getFileLength(path);
                                String[] hosts = fsFactory.getBlockHosts(path, 0, Long.MAX_VALUE);
                                PixelsSplit pixelsSplit = new PixelsSplit(
                                        path, 0, 1, false,
                                        null, order.getColumnOrder(), fileLength, hosts);
                                pixelsSplits.add(pixelsSplit);
                            }
                            // 4. add splits in compactPath
                            int curFileRGIdx;
                            for (Path path : fsFactory.listFiles(layout.getCompactPath()))
                            {
                                long fileLength = fsFactory.getFileLength(path);
                                curFileRGIdx = 0;
                                while (curFileRGIdx < rowGroupNum)
                                {
                                    String node = fileLocations.get(path.toString());
                                    String[] hosts = {node};
                                    PixelsSplit pixelsSplit = new PixelsSplit(path, curFileRGIdx, splitSize,
                                            true, cacheColumnletOrders, order.getColumnOrder(),
                                            fileLength, hosts);
                                    pixelsSplits.add(pixelsSplit);
                                    curFileRGIdx += splitSize;
                                }
                            }
                        }
                        catch (FSException e)
                        {
                            log.error("Failed to open or read HDFS file.", e);
                            return null;
                        }
                    }
                    else
                    {
                        log.error("Get caching files error when version is " + cacheVersion);
                        return null;
                    }
                }
                else
                {
                    log.error("pixels cache version not found.");
                    return null;
                }
            }
            else
            {
                log.debug("cache is disabled");
                List<Path> orderedPaths;
                List<Path> compactPaths;
                try
                {
                    orderedPaths = fsFactory.listFiles(layout.getOrderPath());
                    compactPaths = fsFactory.listFiles(layout.getCompactPath());

                    // add splits in orderedPath
                    for (Path path : orderedPaths)
                    {
                        String[] hosts = fsFactory.getBlockHosts(path, 0, Long.MAX_VALUE);
                        PixelsSplit pixelsSplit = new PixelsSplit(path, 0, 1,
                                false, null, order.getColumnOrder(), fsFactory.getFileLength(path), hosts);
                        pixelsSplits.add(pixelsSplit);
                    }
                    // add splits in compactPath
                    int curFileRGIdx;
                    for (Path path : compactPaths)
                    {
                        curFileRGIdx = 0;
                        while (curFileRGIdx < rowGroupNum)
                        {
                            String[] hosts = fsFactory.getBlockHosts(path, 0, Long.MAX_VALUE);
                            PixelsSplit pixelsSplit = new PixelsSplit(path, curFileRGIdx, splitSize,
                                    false, null, order.getColumnOrder(), splitSize, hosts);
                            pixelsSplits.add(pixelsSplit);
                            curFileRGIdx += splitSize;
                        }
                    }
                }
                catch (FSException e)
                {
                    log.error("Failed to open or read HDFS file.", e);
                    return null;
                }
            }
        }
        sw.stop();
        if (log.isDebugEnabled())
        {
            log.debug("Total # of splits generated by getSplits: " + pixelsSplits.size()
                    + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
        }
        for (PixelsSplit split : pixelsSplits)
        {
            log.error(split);
        }
        return pixelsSplits.toArray(new PixelsSplit[pixelsSplits.size()]);
    }

    /**
     * Get schema and table names from job. The input path of the job should be
     * /pixels/db_name/table_name/...
     * @param job
     * @return.
     */
    private SchemaTableName getSchemaTableName(JobConf job)
    {
        String dirs = job.get(org.apache.hadoop.mapreduce.lib.input.
                FileInputFormat.INPUT_DIR, "");
        assert dirs != null && dirs.length() > 0;
        dirs = dirs.split(",")[0];
        if (dirs.startsWith("hdfs://"))
        {
            dirs = dirs.substring(7);
        } else if(dirs.startsWith("file://"))
        {
            dirs = dirs.substring(6);
        }
        dirs = dirs.substring(dirs.indexOf('/'));
        String[] tokens = dirs.split("/");
        return new SchemaTableName(tokens[2], tokens[3]);
    }

    private Inverted getInverted(Order order, Splits splits, IndexEntry indexEntry) {
        List<String> columnOrder = order.getColumnOrder();
        Inverted index = null;
        try {
            index = new Inverted(columnOrder, AccessPattern.buildPatterns(columnOrder, splits), splits.getNumRowGroupInBlock());
            IndexFactory.Instance().cacheIndex(indexEntry, index);
        } catch (IOException e) {
            log.error("getInverted error: " + e.getMessage());
        }
        return index;
    }

    /**
     * Hive depends on guava-14.0.1, which is not compatible with guava-21.0 used by grpc.
     * So we have to used daos instead of grpc, although it is ugly.
     * TODO: try dynamically unload guava-14.0.1 and load guava-21.1.
     * @param st
     * @return
     */
    private List<Layout> getLayouts(SchemaTableName st)
    {
        SchemaDao schemaDao = new SchemaDao();
        TableDao tableDao = new TableDao();
        LayoutDao layoutDao = new LayoutDao();
        MetadataProto.Schema schema = schemaDao.getByName(st.getSchemaName());
        MetadataProto.Table table = tableDao.getByNameAndSchema(st.getTableName(), schema);
        List<MetadataProto.Layout> layouts = layoutDao.getByTable(table, -1,
                MetadataProto.GetLayoutRequest.PermissionRange.READABLE); // version < 0 means get all versions
        List<Layout> res = new ArrayList<>();
        layouts.forEach(layout -> res.add(new Layout(layout)));
        return res;
    }
}
