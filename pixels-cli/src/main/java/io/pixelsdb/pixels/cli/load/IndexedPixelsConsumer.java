 /*
  * Copyright 2025 PixelsDB.
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
 import io.pixelsdb.pixels.common.exception.IndexException;
 import io.pixelsdb.pixels.common.exception.MetadataException;
 import io.pixelsdb.pixels.common.index.IndexService;
 import io.pixelsdb.pixels.common.index.RPCIndexService;
 import io.pixelsdb.pixels.common.index.RowIdAllocator;
 import io.pixelsdb.pixels.common.metadata.domain.File;
 import io.pixelsdb.pixels.common.metadata.domain.Path;
 import io.pixelsdb.pixels.common.node.BucketCache;
 import io.pixelsdb.pixels.common.physical.Storage;
 import io.pixelsdb.pixels.common.physical.StorageFactory;
 import io.pixelsdb.pixels.common.utils.ConfigFactory;
 import io.pixelsdb.pixels.common.utils.DateUtil;
 import io.pixelsdb.pixels.common.utils.RetinaUtils;
 import io.pixelsdb.pixels.core.PixelsWriter;
 import io.pixelsdb.pixels.core.TypeDescription;
 import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
 import io.pixelsdb.pixels.daemon.NodeProto;
 import io.pixelsdb.pixels.index.IndexProto;

 import java.io.BufferedReader;
 import java.io.IOException;
 import java.io.InputStreamReader;
 import java.nio.ByteBuffer;
 import java.util.ArrayList;
 import java.util.LinkedList;
 import java.util.List;
 import java.util.Map;
 import java.util.concurrent.BlockingQueue;
 import java.util.concurrent.ConcurrentHashMap;
 import java.util.concurrent.ConcurrentLinkedQueue;
 import java.util.regex.Pattern;

 /**
  * Consumer implementation for loading data with Primary Index (Index != null).
  * It routes data rows to different PixelsFiles based on their calculated bucketId.
  */
 public class IndexedPixelsConsumer extends AbstractPixelsConsumer
 {

     // Map: Bucket ID -> Writer state
     private final Map<Integer, PerBucketWriter> bucketWriters = new ConcurrentHashMap<>();
     private final BucketCache bucketCache = BucketCache.getInstance();
     private final Map<NodeProto.NodeInfo, IndexService> indexServices = new ConcurrentHashMap<>();
     private final int indexServerPort;

     public IndexedPixelsConsumer(BlockingQueue<String> queue, Parameters parameters,
                                  ConcurrentLinkedQueue<LoadedInfo> loadedInfos)
     {
         super(queue, parameters, loadedInfos);
         ConfigFactory config = ConfigFactory.Instance();
         this.indexServerPort = Integer.parseInt(config.getProperty("index.server.port"));
     }

     @Override
     protected void processSourceFile(String originalFilePath) throws IOException, MetadataException
     {
         Storage originStorage = StorageFactory.Instance().getStorage(originalFilePath);
         try (BufferedReader reader = new BufferedReader(new InputStreamReader(originStorage.open(originalFilePath))))
         {

             System.out.println("loading indexed data from: " + originalFilePath);
             long timestamp = parameters.getTimestamp();
             String line;

             while ((line = reader.readLine()) != null)
             {
                 if (line.isEmpty())
                 {
                     System.err.println("thread: " + currentThread().getName() + " got empty line.");
                     continue;
                 }

                 String[] colsInLine = line.split(Pattern.quote(regex));

                 // 1. Calculate Primary Key and Bucket ID
                 ByteString pkByteString = calculatePrimaryKeyBytes(colsInLine);
                 // Assume BucketCache has the necessary method and configuration
                 int bucketId = RetinaUtils.getBucketIdFromByteBuffer(pkByteString);

                 // 2. Get/Initialize the Writer for this Bucket
                 PerBucketWriter bucketWriter = bucketWriters.computeIfAbsent(bucketId, id ->
                 {
                     try
                     {
                         return initializeBucketWriter(id);
                     } catch (Exception e)
                     {
                         throw new RuntimeException("Failed to initialize writer for bucket " + id, e);
                     }
                 });

                 // 3. Write Data Row
                 writeRowToBatch(bucketWriter.rowBatch, colsInLine, timestamp);
                 bucketWriter.rowCounter++;

                 try
                 {
                     // 4. Update Index Entry
                     updateIndexEntry(bucketWriter, pkByteString);

                     // 5. Check and Flush Row Batch
                     if (bucketWriter.rowBatch.size >= bucketWriter.rowBatch.getMaxSize())
                     {
                         flushRowBatch(bucketWriter);
                     }

                     // 6. Check and Close File
                     if (bucketWriter.rowCounter >= maxRowNum)
                     {
                         closePixelsFile(bucketWriter);
                         // Remove writer to force re-initialization on next use
                         bucketWriters.remove(bucketId);
                     }
                 } catch (IndexException e)
                 {
                     e.printStackTrace();
                 }
             }
         }
     }

     @Override
     protected void flushRemainingData() throws IOException, MetadataException
     {
         for (PerBucketWriter bucketWriter : bucketWriters.values())
         {
             if (bucketWriter.rowCounter > 0)
             {
                 try
                 {
                     closePixelsFile(bucketWriter);
                 } catch (IndexException e)
                 {
                     e.printStackTrace();
                 }
             }
         }
         bucketWriters.clear();
     }

     /**
      * Initializes a new PixelsWriter and associated File/Path for a given bucket ID.
      */
     private PerBucketWriter initializeBucketWriter(int bucketId) throws IOException, MetadataException
     {
         // Use the Node Cache to find the responsible Retina Node
         NodeProto.NodeInfo targetNode = bucketCache.getRetinaNodeInfoByBucketId(bucketId);

         // Target path selection logic (simple round-robin for the path, but the NodeInfo is bucket-specific)
         int targetPathId = GlobalTargetPathId.getAndIncrement() % targetPaths.size();
         Path currTargetPath = targetPaths.get(targetPathId);
         String targetDirPath = currTargetPath.getUri();
         Storage targetStorage = StorageFactory.Instance().getStorage(targetDirPath);

         if (!targetDirPath.endsWith("/"))
         {
             targetDirPath += "/";
         }
         String targetFileName = targetNode.getAddress() + "_" + DateUtil.getCurTime() + "_" + bucketId + ".pxl";
         String targetFilePath = targetDirPath + targetFileName;

         PixelsWriter pixelsWriter = getPixelsWriter(targetStorage, targetFilePath);

         File currFile = openTmpFile(targetFileName, currTargetPath);
         tmpFiles.add(currFile);

         return new PerBucketWriter(pixelsWriter, currFile, currTargetPath, targetNode);
     }

     // --- Private Helper Methods ---

     private ByteString calculatePrimaryKeyBytes(String[] colsInLine)
     {
         TypeDescription pkTypeDescription = parameters.getPkTypeDescription();
         List<byte[]> pkBytes = new LinkedList<>();
         int indexKeySize = 0;

         for (int i = 0; i < pkMapping.length; i++)
         {
             int pkColumnId = pkMapping[i];
             // Safety check for array bounds
             if (pkColumnId >= colsInLine.length)
             {
                 throw new IllegalArgumentException("Primary key mapping index out of bounds for line.");
             }
             byte[] bytes = pkTypeDescription.getChildren().get(i).convertSqlStringToByte(colsInLine[pkColumnId]);
             pkBytes.add(bytes);
             indexKeySize += bytes.length;
         }

         ByteBuffer indexKeyBuffer = ByteBuffer.allocate(indexKeySize);
         for (byte[] pkByte : pkBytes)
         {
             indexKeyBuffer.put(pkByte);
         }
         return ByteString.copyFrom((ByteBuffer) indexKeyBuffer.rewind());
     }

     private void updateIndexEntry(PerBucketWriter bucketWriter, ByteString pkByteString) throws IndexException
     {
         IndexProto.PrimaryIndexEntry.Builder builder = IndexProto.PrimaryIndexEntry.newBuilder();
         builder.getIndexKeyBuilder()
                 .setTimestamp(parameters.getTimestamp())
                 .setKey(pkByteString)
                 .setIndexId(index.getId())
                 .setTableId(index.getTableId());

         builder.setRowId(bucketWriter.rowIdAllocator.getRowId());
         builder.getRowLocationBuilder()
                 .setRgId(bucketWriter.rgId)
                 .setFileId(bucketWriter.currFile.getId())
                 .setRgRowOffset(bucketWriter.rgRowOffset++);

         bucketWriter.indexEntries.add(builder.build());
     }

     private void flushRowBatch(PerBucketWriter bucketWriter) throws IOException, IndexException
     {
         bucketWriter.pixelsWriter.addRowBatch(bucketWriter.rowBatch);
         bucketWriter.rowBatch.reset();

         bucketWriter.rgId = bucketWriter.pixelsWriter.getNumRowGroup();
         if (bucketWriter.prevRgId != bucketWriter.rgId)
         {
             bucketWriter.rgRowOffset = 0;
             bucketWriter.prevRgId = bucketWriter.rgId;
         }

         // Push index entries to the corresponding IndexService (determined by targetNode address)
         bucketWriter.indexService.putPrimaryIndexEntries(index.getTableId(), index.getId(), bucketWriter.indexEntries);
         bucketWriter.indexEntries.clear();

         bucketWriter.indexService.flushIndexEntriesOfFile(index.getTableId(), index.getId(),bucketWriter.currFile.getId(), true);
     }

     private void closePixelsFile(PerBucketWriter bucketWriter) throws IOException, MetadataException, IndexException
     {
         // Final flush of remaining rows/indexes
         if (bucketWriter.rowBatch.size != 0)
         {
             flushRowBatch(bucketWriter);
         }

         closeWriterAndAddFile(bucketWriter.pixelsWriter, bucketWriter.currFile, bucketWriter.currTargetPath, bucketWriter.targetNode);
     }

     private class PerBucketWriter
     {
         PixelsWriter pixelsWriter;
         File currFile;
         Path currTargetPath;
         int rgId;
         int rgRowOffset;
         int prevRgId;
         int rowCounter;
         NodeProto.NodeInfo targetNode;
         List<IndexProto.PrimaryIndexEntry> indexEntries = new ArrayList<>();
         VectorizedRowBatch rowBatch;
         IndexService indexService;
         RowIdAllocator rowIdAllocator;

         public PerBucketWriter(PixelsWriter writer, File file, Path path, NodeProto.NodeInfo node)
         {
             this.pixelsWriter = writer;
             this.currFile = file;
             this.currTargetPath = path;
             this.targetNode = node;
             this.rgId = writer.getNumRowGroup();
             this.prevRgId = this.rgId;
             this.rgRowOffset = 0;
             this.rowCounter = 0;
             this.rowBatch = schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
             this.indexService = indexServices.computeIfAbsent(node, nodeInfo ->
                     RPCIndexService.CreateInstance(nodeInfo.getAddress(), indexServerPort));
             this.rowIdAllocator = new RowIdAllocator(index.getTableId(), 1000, this.indexService);
         }
     }
 }