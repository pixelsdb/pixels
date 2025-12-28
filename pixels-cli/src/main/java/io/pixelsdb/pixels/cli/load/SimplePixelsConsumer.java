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


 import io.pixelsdb.pixels.common.exception.MetadataException;
 import io.pixelsdb.pixels.common.metadata.domain.File;
 import io.pixelsdb.pixels.common.metadata.domain.Path;
 import io.pixelsdb.pixels.common.physical.Storage;
 import io.pixelsdb.pixels.common.physical.StorageFactory;
 import io.pixelsdb.pixels.common.utils.Constants;
 import io.pixelsdb.pixels.common.utils.DateUtil;
 import io.pixelsdb.pixels.core.PixelsWriter;
 import io.pixelsdb.pixels.core.PixelsWriterImpl;
 import io.pixelsdb.pixels.core.TypeDescription;
 import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

 import java.io.BufferedReader;
 import java.io.IOException;
 import java.io.InputStreamReader;
 import java.util.concurrent.BlockingQueue;
 import java.util.concurrent.ConcurrentLinkedQueue;


 /**
  * Consumer implementation for loading data without any Primary Index (Index == null).
  * Uses simple round-robin path selection.
  */
 public class SimplePixelsConsumer extends AbstractPixelsConsumer
 {

     private final VectorizedRowBatch rowBatch;
     private PixelsWriter pixelsWriter;
     private File currFile;
     private Path currTargetPath;
     private int rowCounter = 0;

     public SimplePixelsConsumer(BlockingQueue<String> queue, Parameters parameters,
                                 ConcurrentLinkedQueue<LoadedInfo> loadedInfos)
     {
         super(queue, parameters, loadedInfos);
         this.rowBatch = schema.createRowBatchWithHiddenColumn(pixelStride, TypeDescription.Mode.NONE);
     }

     @Override
     protected void processSourceFile(String originalFilePath) throws IOException, MetadataException
     {
         Storage originStorage = StorageFactory.Instance().getStorage(originalFilePath);
         try (BufferedReader reader = new BufferedReader(new InputStreamReader(originStorage.open(originalFilePath))))
         {
             System.out.println("loading data from: " + originalFilePath);
             long timestamp = parameters.getTimestamp();
             String line;

             while ((line = reader.readLine()) != null)
             {
                 if (line.isEmpty())
                 {
                     System.err.println("thread: " + currentThread().getName() + " got empty line.");
                     continue;
                 }

                 // 1. Check if a new PixelsFile needs to be initialized
                 if (pixelsWriter == null)
                 {
                     initializePixelsWriter();
                 }

                 // 2. Write row to batch
                 writeRowToBatch(rowBatch, line, timestamp);
                 rowCounter++;

                 // 3. Check and Flush Row Batch
                 if (rowBatch.size >= rowBatch.getMaxSize())
                 {
                     pixelsWriter.addRowBatch(rowBatch);
                     rowBatch.reset();
                 }

                 // 4. Check and Close File
                 if (rowCounter >= maxRowNum)
                 {
                     closePixelsFile();
                 }
             }
         }
     }

     @Override
     protected void flushRemainingData() throws IOException
     {
         if (rowCounter > 0)
         {
             closePixelsFile();
         }
     }

     // --- Private Helper Methods for Simple Consumer ---

     private void initializePixelsWriter() throws MetadataException, IOException
     {
         int targetPathId = GlobalTargetPathId.getAndIncrement() % targetPaths.size();
         currTargetPath = targetPaths.get(targetPathId);
         String targetDirPath = currTargetPath.getUri();
         Storage targetStorage = StorageFactory.Instance().getStorage(targetDirPath);

         if (!targetDirPath.endsWith("/"))
         {
             targetDirPath += "/";
         }
         String targetFileName = Constants.LOAD_DEFAULT_RETINA_PREFIX + "_" + DateUtil.getCurTime() + ".pxl";
         String targetFilePath = targetDirPath + targetFileName;

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
         rowCounter = 0;
     }

     private void closePixelsFile() throws IOException
     {
         if (rowBatch.size != 0)
         {
             pixelsWriter.addRowBatch(rowBatch);
             rowBatch.reset();
         }
         closeWriterAndAddFile(pixelsWriter, currFile, currTargetPath, null);

         // Reset state for next file
         pixelsWriter = null;
         currFile = null;
         rowCounter = 0;
     }
 }