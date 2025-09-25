/*
 * Copyright 2018-2019 PixelsDB.
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
package io.pixelsdb.pixels.cli.load;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.common.index.RowIdAllocator;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.index.IndexProto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: tao
 * @author hank
 * @create in 2018-10-30 15:18
 **/
public class PixelsConsumer extends Consumer
{
    public static final AtomicInteger GlobalTargetPathId = new AtomicInteger(0);
    private final BlockingQueue<String> queue;
    private final Parameters parameters;
    private final ConcurrentLinkedQueue<File> loadedFiles;
    private final ConcurrentLinkedQueue<Path> loadedPaths;
    private final List<File> tmpFiles = new ArrayList<>();
    private MetadataService metadataService;

    public PixelsConsumer(BlockingQueue<String> queue, Parameters parameters,
                          ConcurrentLinkedQueue<File> loadedFiles, ConcurrentLinkedQueue<Path> loadedPaths)
    {
        this.queue = queue;
        this.parameters = parameters;
        this.loadedFiles = loadedFiles;
        this.loadedPaths = loadedPaths;
        this.metadataService = parameters.getMetadataService();
    }

    @Override
    public void run()
    {
        System.out.println("Start PixelsConsumer, " + currentThread().getName() +
                ", time: " + DateUtil.formatTime(new Date()));
        int count = 0;

        boolean isRunning = true;
        try
        {
            final List<Path> targetPaths = parameters.getLoadingPaths();
            String schemaStr = parameters.getSchema();
            int[] orderMapping = parameters.getOrderMapping();
            int maxRowNum = parameters.getMaxRowNum();
            String regex = parameters.getRegex();
            EncodingLevel encodingLevel = parameters.getEncodingLevel();
            boolean nullsPadding = parameters.isNullsPadding();
            RowIdAllocator rowIdAllocator = parameters.getRowIdAllocator();
            int[] pkMapping = parameters.getPkMapping();
            SinglePointIndex index = parameters.getIndex();
            IndexService indexService = IndexService.Instance();

            if (regex.equals("\\s"))
            {
                regex = " ";
            }

            ConfigFactory configFactory = ConfigFactory.Instance();
            int pixelStride = Integer.parseInt(configFactory.getProperty("pixel.stride"));
            int rowGroupSize = Integer.parseInt(configFactory.getProperty("row.group.size"));
            long blockSize = Long.parseLong(configFactory.getProperty("block.size"));
            short replication = Short.parseShort(configFactory.getProperty("block.replication"));

            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
            ColumnVector[] columnVectors = rowBatch.cols;

            BufferedReader reader;
            String line;

            boolean initPixelsFile = true;
            String targetFileName = null;
            String targetFilePath;
            PixelsWriter pixelsWriter = null;
            Path currTargetPath = null;
            int rowCounter = 0;
            int rgId = 0;
            int prevRgId = 0;
            int rgRowOffset = 0;
            File currFile = null;
            List<IndexProto.PrimaryIndexEntry> indexEntries = new ArrayList<>();

            while (isRunning)
            {
                String originalFilePath = queue.poll(2, TimeUnit.SECONDS);
                if (originalFilePath != null)
                {
                    count++;
                    Storage originStorage = StorageFactory.Instance().getStorage(originalFilePath);
                    reader = new BufferedReader(new InputStreamReader(originStorage.open(originalFilePath)));

                    System.out.println("loading data from: " + originalFilePath);

                    // loaded rows use the same timestamp
                    long timestamp = parameters.getTimestamp();
                    while ((line = reader.readLine()) != null)
                    {
                        if(line.isEmpty())
                        {
                            System.err.println("thread: " + currentThread().getName() + " got empty line.");
                            continue;
                        }

                        if (initPixelsFile)
                        {
                            // we create a new pixels file if we can read a next line from the source file

                            // choose the target output directory using round-robin
                            int targetPathId = GlobalTargetPathId.getAndIncrement() % targetPaths.size();
                            currTargetPath = targetPaths.get(targetPathId);
                            String targetDirPath = currTargetPath.getUri();
                            Storage targetStorage = StorageFactory.Instance().getStorage(targetDirPath);

                            if (!targetDirPath.endsWith("/"))
                            {
                                targetDirPath += "/";
                            }
                            targetFileName = DateUtil.getCurTime() + ".pxl";
                            targetFilePath = targetDirPath + targetFileName;

                            pixelsWriter = PixelsWriterImpl.newBuilder()
                                    .setSchema(schema)
                                    .setHasHiddenColumn(true)
                                    .setPixelStride(pixelStride)
                                    .setRowGroupSize(rowGroupSize)
                                    .setStorage(targetStorage)
                                    .setPath(targetFilePath)
                                    .setBlockSize(blockSize)
                                    .setReplication(replication)
                                    .setBlockPadding(true)
                                    .setEncodingLevel(encodingLevel)
                                    .setNullsPadding(nullsPadding)
                                    .setCompressionBlockSize(1)
                                    .build();

                            currFile = openTmpFile(targetFileName, currTargetPath);
                            tmpFiles.add(currFile);
                            rgId = pixelsWriter.getNumRowGroup();
                            rgRowOffset = 0;
                        }
                        initPixelsFile = false;

                        rowBatch.size++;
                        rowCounter++;

                        String[] colsInLine = line.split(regex);
                        for (int i = 0; i < columnVectors.length - 1; i++)
                        {
                            try
                            {
                                int valueIdx = orderMapping[i];
                                if (valueIdx >= colsInLine.length ||
                                        colsInLine[valueIdx].isEmpty() ||
                                        colsInLine[valueIdx].equalsIgnoreCase("\\N"))
                                {
                                    columnVectors[i].addNull();
                                } else
                                {
                                    columnVectors[i].add(colsInLine[valueIdx]);
                                }
                            }
                            catch (Exception e)
                            {
                                System.out.println("line: " + line);
                                e.printStackTrace();
                            }
                        }
                        // add hidden timestamp column value
                        columnVectors[columnVectors.length - 1].add(timestamp);

                        if(index != null)
                        {
                            // TODO: Support Secondary Index
                            int indexKeySize = 0;
                            for(int pkColumnId : pkMapping)
                            {
                                indexKeySize += colsInLine[pkColumnId].length();
                            }
                            indexKeySize += Long.BYTES + (pkMapping.length + 1) * 2; // table id + index key

                            TypeDescription pkTypeDescription = parameters.getPkTypeDescription();
                            List<byte[]> pkBytes = new LinkedList<>();
                            for(int i = 0; i < pkMapping.length; i++)
                            {
                                int pkColumnId = pkMapping[i];
                                byte[] bytes = pkTypeDescription.getChildren().get(i).convertSqlStringToByte(colsInLine[pkColumnId]);
                                pkBytes.add(bytes);
                                indexKeySize += bytes.length;
                            }
                            ByteBuffer indexKeyBuffer = ByteBuffer.allocate(indexKeySize);
                            indexKeyBuffer.putLong(index.getTableId()).putChar(':');
                            for(byte[] pkByte : pkBytes)
                            {
                                indexKeyBuffer.put(pkByte);
                                indexKeyBuffer.putChar(':');
                            }
                            IndexProto.PrimaryIndexEntry.Builder builder = IndexProto.PrimaryIndexEntry.newBuilder();
                            builder.getIndexKeyBuilder()
                                    .setTimestamp(parameters.getTimestamp())
                                    .setKey(ByteString.copyFrom((ByteBuffer) indexKeyBuffer.rewind()))
                                    .setIndexId(index.getId())
                                    .setTableId(index.getTableId());
                            builder.setRowId(rowIdAllocator.getRowId());
                            builder.getRowLocationBuilder()
                                    .setRgId(rgId)
                                    .setFileId(currFile.getId())
                                    .setRgRowOffset(rgRowOffset++);
                            indexEntries.add(builder.build());
                        }

                        if (rowBatch.size >= rowBatch.getMaxSize())
                        {
                            pixelsWriter.addRowBatch(rowBatch);
                            rowBatch.reset();
                            rgId = pixelsWriter.getNumRowGroup();

                            if(prevRgId != rgId)
                            {
                                rgRowOffset = 0;
                                prevRgId = rgId;
                            }

                            if(index != null)
                            {
                                indexService.putPrimaryIndexEntries(index.getTableId(), index.getId(), indexEntries);
                                indexEntries.clear();
                            }
                        }

                        if (rowCounter >= maxRowNum)
                        {
                            // finish writing the file
                            if (rowBatch.size != 0)
                            {
                                pixelsWriter.addRowBatch(rowBatch);
                                rowBatch.reset();
                                if(index != null)
                                {
                                    indexService.putPrimaryIndexEntries(index.getTableId(), index.getId(), indexEntries);
                                    indexEntries.clear();
                                }
                            }
                            closeWriterAndAddFile(pixelsWriter, currFile, currTargetPath);
                            rowCounter = 0;
                            initPixelsFile = true;
                        }
                    }
                    reader.close();
                } else
                {
                    // no source file can be consumed within 2 seconds,
                    // loading is considered to be finished.
                    isRunning = false;
                }
            }

            if (rowCounter > 0)
            {
                // last file to write
                if (rowBatch.size != 0)
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                    if(index != null)
                    {
                        indexService.putPrimaryIndexEntries(index.getTableId(), index.getId(), indexEntries);
                        indexEntries.clear();
                    }
                }
                closeWriterAndAddFile(pixelsWriter, currFile, currTargetPath);
            }
        } catch (InterruptedException e)
        {
            System.out.println("PixelsConsumer: " + e.getMessage());
            currentThread().interrupt();
        } catch (Throwable e)
        {
            e.printStackTrace();
        } finally
        {
            for(File tmpFile: tmpFiles)
            {
                // If the file is successfully loaded, its type should have been locally modified to REGULAR.
                // Otherwise, TEMPORARY files need to be cleaned up in the Metadata Service.
                if(tmpFile.getType() == File.Type.TEMPORARY)
                {
                    try
                    {
                        metadataService.deleteFiles(Collections.singletonList((tmpFile.getId())));
                    } catch (MetadataException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println(currentThread().getName() + ":" + count);
            System.out.println("Exit PixelsConsumer, thread: " + currentThread().getName() +
                    ", time: " + DateUtil.formatTime(new Date()));
        }
    }

    /**
     * Close the pixels writer and add the file to loaded file queue.
     * Files in the loaded files queue will be updated in metadata.
     * @param pixelsWriter the pixels writer
     * @param loadedFile the file name has been loaded
     * @param filePath the path of the directory where the file was written
     * @throws IOException
     */
    private void closeWriterAndAddFile(PixelsWriter pixelsWriter, File loadedFile, Path filePath) throws IOException
    {
        pixelsWriter.close();
        loadedFile.setType(File.Type.REGULAR);
        loadedFile.setNumRowGroup(pixelsWriter.getNumRowGroup());
        this.loadedFiles.offer(loadedFile);
        this.loadedPaths.offer(filePath);
    }

    /**
     * Create a temporary file through the metadata service
     * @param fileName the file name without directory path
     * @param filePath the path of the directory where the file was written
     */
    private File openTmpFile(String fileName, Path filePath) throws MetadataException
    {
        File file = new File();
        file.setName(fileName);
        file.setType(File.Type.TEMPORARY);
        file.setNumRowGroup(1);
        file.setPathId(filePath.getId());
        String tmpFilePath = filePath.getUri() + "/" + fileName;
        this.metadataService.addFiles(Collections.singletonList(file));
        file.setId(metadataService.getFileId(tmpFilePath));
        return file;
    }
}
