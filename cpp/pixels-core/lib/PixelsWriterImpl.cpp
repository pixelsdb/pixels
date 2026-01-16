/*
 * Copyright 2024 PixelsDB.
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

/*
 * @author gengdy
 * @create 2024-11-25
 */
#include "PixelsWriterImpl.h"
#include "ColumnWriterBuilder.h"
#include "PixelsVersion.h"
#include "encoding/EncodingLevel.h"
#include "physical/PhysicalReader.h"
#include "physical/PhysicalReaderUtil.h"
#include "PixelsVersion.h"
#include "utils/Endianness.h"
#include "physical/PhysicalWriterUtil.h"
#include "reader/PixelsRecordReader.h"
#include "reader/PixelsRecordReaderImpl.h"
#include "utils/Endianness.h"
#include "writer/ColumnWriterBuilder.h"
#include <future>
#include <string>

const int PixelsWriterImpl::CHUNK_ALIGNMENT =
    std::stoi(ConfigFactory::Instance().getProperty("column.chunk.alignment"));

const std::vector<uint8_t> PixelsWriterImpl::CHUNK_PADDING_BUFFER =
    std::vector<uint8_t>(CHUNK_ALIGNMENT, 0);

PixelsWriterImpl::PixelsWriterImpl(std::shared_ptr<TypeDescription> schema,
                                   int pixelsStride, int rowGroupSize,
                                   const std::string &targetFilePath,
                                   int blockSize, bool blockPadding,
                                   EncodingLevel encodingLevel,
                                   bool nullsPadding, bool partitioned,
                                   int compressionBlockSize)
    : schema(schema), rowGroupSize(rowGroupSize),
      compressionBlockSize(compressionBlockSize),fbb(1024)
{
  this->columnWriterOption = std::make_shared<PixelsWriterOption>()
      ->setPixelsStride(pixelsStride)
      ->setEncodingLevel(encodingLevel)
      ->setNullsPadding(nullsPadding);
  this->physicalWriter = PhysicalWriterUtil::newPhysicalWriter(
      targetFilePath, blockSize, blockPadding, false);
  this->compressionKind = pixels::fb::CompressionKind::CompressionKind_NONE;
  // this->timeZone =
  // std::unique_ptr<icu::TimeZone>(icu::TimeZone::createDefault());
  this->children = schema->getChildren();
  this->partitioned = partitioned;

  for (int i = 0; i < children.size(); i++)
  {
    columnWriters.push_back(ColumnWriterBuilder::newColumnWriter(
        children.at(i), columnWriterOption));
  }
}

bool PixelsWriterImpl::addRowBatch(
    std::shared_ptr<VectorizedRowBatch> rowBatch)
{
  curRowGroupDataLength = 0;
  curRowGroupNumOfRows += rowBatch->count();
  writeColumnVectors(rowBatch->cols, rowBatch->count());

  if (curRowGroupDataLength >= rowGroupSize)
  {
    writeRowGroup();
    curRowGroupNumOfRows = 0L;
    return false;
  }
  return true;
}
// single thread
// void PixelsWriterImpl::writeColumnVectors(
//     std::vector<std::shared_ptr<ColumnVector>> &columnVectors,
//     int rowBatchSize)
// {
//
//   int batchDataLength = 0;
//   int commonColumnLength = columnVectors.size();
//
//
//   for (int i = 0; i < commonColumnLength; ++i)
//   {
//     try
//     {
//       int columnWrittenSize = columnWriters[i]->write(columnVectors[i], rowBatchSize);
//       batchDataLength += columnWrittenSize;
//     }
//     catch (const std::exception &e)
//     {
//       throw std::runtime_error("Single-threaded write failed at column [" + std::to_string(i) +
//           "], name: " + ". Error: " + std::string(e.what()));
//     }
//   }
//   curRowGroupDataLength += batchDataLength;
// }

void PixelsWriterImpl::writeColumnVectors(
    std::vector<std::shared_ptr<ColumnVector>> &columnVectors,
    int rowBatchSize)
{
  std::vector<std::future<int>> futures;
  int commonColumnLength = columnVectors.size();

  // Writing regular columns in parallel
  // Each column writer now maintains its own statistics snapshots
  // and doesn't need the shared FlatBufferBuilder until buildColumnChunkIndex
  for (int i = 0; i < commonColumnLength; ++i)
  {
    futures.emplace_back(std::async(std::launch::async, [this, &columnVectors,
        rowBatchSize, i]()
    {
      try
      {
        // Each thread uses a temporary FlatBufferBuilder for newPixel serialization
        // The actual usage is internal to write() -> newPixel()
        return columnWriters[i]->write(columnVectors[i], rowBatchSize);
      }
      catch (const std::exception &e)
      {
        throw std::runtime_error("failed to write column vector [" + std::to_string(i) + "]: " +
            std::string(e.what()));
      }
    }));
  }

  // Wait for all futures to complete and accumulate data length
  int dataLength = 0;
  for (auto &future : futures)
  {
    dataLength += future.get(); // Blocking until all tasks are completed
  }

  // Accumulate current RowGroup data length
  curRowGroupDataLength += dataLength;
}
void PixelsWriterImpl::close()
{
  try
  {
    if (curRowGroupNumOfRows != 0)
    {
      writeRowGroup();
    }
    writeFileTail();
    physicalWriter->close();
    for (auto cw : columnWriters)
    {
      cw->close();
    }
  } catch (const std::exception &e)
  {
    std::cerr << e.what() << std::endl;
    throw;
  }
}




/*
 * writeRowGroup
 *
 * Persist the current RowGroup (accumulated through multiple addRowBatch() calls)
 * to disk, and collect its metadata to be written to the file footer.
 *
 * Trigger conditions:
 * 1) During addRowBatch(), when the accumulated RowGroup data size reaches
 *    the threshold (curRowGroupDataLength >= rowGroupSize)
 * 2) Before close(), when there are remaining unwritten rows
 *    (curRowGroupNumOfRows != 0)
 *
 * The current implementation is divided into four stages
 * (corresponding to the four private methods below):
 *
 * 1) prepareRowGroup()
 *    - Flush each ColumnWriter, materializing its internal buffers into
 *      column chunks
 *    - Compute the total data length to be written for this RowGroup,
 *      including alignment padding based on CHUNK_ALIGNMENT
 *
 * 2) writeRowGroupData(rowGroupDataLength)
 *    - Call physicalWriter->prepare() to reserve write space and obtain
 *      curRowGroupOffset
 *    - Insert alignment padding at the beginning of the RowGroup if needed
 *      (to avoid block boundary or alignment issues)
 *    - Sequentially write the raw data of each column chunk, inserting
 *      padding after each chunk when required
 *
 * 3) writeRowGroupFooter()
 *    - Use FlatBuffers (via the member fbb) to build the ColumnChunkIndex
 *      and Encoding information for this RowGroup
 *    - Reinitialize columnWriters[i] to begin writing the next RowGroup
 *    - Return the RowGroup data region length, which matches the value
 *      computed in prepareRowGroup() and is used for subsequent metadata
 *
 * 4) recordRowGroupMetadata(rowGroupDataLength)
 *    - Serialize the completed RowGroupFooter stored in fbb and write it
 *      to the physical file
 *    - Update rowGroupInfoList / rowGroupMetadataList as well as
 *      fileRowNum and fileContentLength
 *    - Finally, call fbb.Clear() to reset the builder state, preventing
 *      memory and object contamination from reuse
 */

void PixelsWriterImpl::writeRowGroup()
{

  int rowGroupDataLength=prepareRowGroup();

  writeRowGroupData(rowGroupDataLength);

  rowGroupDataLength=writeRowGroupFooter();

  recordRowGroupMetadata(rowGroupDataLength);

}


int PixelsWriterImpl::prepareRowGroup()
{
  int rowGroupDataLength =0;
  for (auto writer: columnWriters)
  {
    // flush residual rows to storage, a new pixels is created
    writer->flush();
    rowGroupDataLength+=writer->getColumnChunkSize();
    if (CHUNK_ALIGNMENT != 0 && rowGroupDataLength % CHUNK_ALIGNMENT != 0)
    {
      /*
       * Issue #519:
       * This is necessary as the prepare() method of some storage (e.g., hdfs)
       * has to determine whether to start a new block, if the current block
       * is not large enough.
       */
      rowGroupDataLength +=
          CHUNK_ALIGNMENT - rowGroupDataLength % CHUNK_ALIGNMENT;
    }
  }
  return rowGroupDataLength;
}

void PixelsWriterImpl::writeRowGroupData(uint32_t rowGroupDataLength)
{
  try
  {
  curRowGroupOffset= physicalWriter->prepare(rowGroupDataLength);
    if (curRowGroupOffset!=-1){
    // No need for double alignment
    if (CHUNK_ALIGNMENT !=0 && curRowGroupOffset % CHUNK_ALIGNMENT)
    {
      int paddingNeeded = CHUNK_ALIGNMENT -(curRowGroupOffset% CHUNK_ALIGNMENT);
      physicalWriter->append(CHUNK_PADDING_BUFFER.data(),0,paddingNeeded);
      writtenBytes += paddingNeeded;
      curRowGroupOffset = physicalWriter->prepare(rowGroupDataLength);
    }

    for (auto & writer: columnWriters)
    {
      auto rowGroupBuffer=writer->getColumnChunkContent();
      physicalWriter->append(rowGroupBuffer.data(),0,rowGroupBuffer.size());
      writtenBytes += rowGroupBuffer.size();
      if (CHUNK_ALIGNMENT != 0 &&
          rowGroupBuffer.size() % CHUNK_ALIGNMENT != 0)
      {
        int alignBytes =
            CHUNK_ALIGNMENT - rowGroupBuffer.size() % CHUNK_ALIGNMENT;
        physicalWriter->append(CHUNK_PADDING_BUFFER.data(), 0, alignBytes);
        writtenBytes += alignBytes;
      }
    }
    physicalWriter->flush();
  } else
  {
    std::cerr << "Write row group prepare failed" << std::endl;
    throw std::runtime_error("Write row group prepare failed");
  }
  }catch (const std::exception &e)
  {
    std::cerr << e.what() << std::endl;
    throw;
  }
}

int PixelsWriterImpl::writeRowGroupFooter()
{
  std::vector<
    flatbuffers::Offset<pixels::fb::ColumnChunkIndex>>
    columnChunkIndexVector;

  std::vector<
      flatbuffers::Offset<pixels::fb::ColumnEncoding>>
      columnChunkEncodingVector;


  // update index and stats(necessary?)
  int rowGroupDataLength = 0;
  for (int i = 0; i < columnWriters.size(); i++)
  {
    std::shared_ptr<ColumnWriter> writer = columnWriters[i];
    auto chunkIndex = writer->buildColumnChunkIndex(fbb,
      curRowGroupOffset + rowGroupDataLength,writer->getColumnChunkSize(),true);

    rowGroupDataLength += writer->getColumnChunkSize();
    if (CHUNK_ALIGNMENT != 0 && rowGroupDataLength % CHUNK_ALIGNMENT != 0)
    {
      rowGroupDataLength +=
          CHUNK_ALIGNMENT - rowGroupDataLength % CHUNK_ALIGNMENT;
    }
    columnChunkIndexVector.push_back(chunkIndex);
    columnChunkEncodingVector.push_back(writer->getColumnChunkEncoding(fbb));

    columnWriters[i] = ColumnWriterBuilder::newColumnWriter(children.at(i),
                                                            columnWriterOption);
  }
  // put curRowGroupIndex into rowGroupFooter
  auto entriesOffset = fbb.CreateVector(columnChunkIndexVector);
  auto curRowGroupIndex = pixels::fb::CreateRowGroupIndex(fbb, entriesOffset);
  auto curRowGroupEncodingOffset = fbb.CreateVector(columnChunkEncodingVector);
  auto curRowGroupEncoding=pixels::fb::CreateRowGroupEncoding(fbb,curRowGroupEncodingOffset,0);
  flatbuffers::Offset<pixels::fb::RowGroupFooter> rowGroupFooterOffset =
    pixels::fb::CreateRowGroupFooter(fbb, curRowGroupIndex, curRowGroupEncoding);

  auto curRowGroupFooterOffset = rowGroupFooterOffset;

  fbb.Finish(curRowGroupFooterOffset);

  return rowGroupDataLength;

}

void PixelsWriterImpl::recordRowGroupMetadata(int rowGroupDataLength )
{
    uint8_t *footerBufPtr = fbb.GetBufferPointer();
  size_t footerBufSize = fbb.GetSize();
  long uploadedFooterOffset = 0;

  try
  {
    physicalWriter->prepare(footerBufSize);
    uploadedFooterOffset = physicalWriter->append(footerBufPtr, 0, footerBufSize);
    writtenBytes += footerBufSize;
    physicalWriter->flush();
  } catch (const std::exception &e)
  {
    std::cerr << "Failed to write RowGroupFooter: " << e.what() << std::endl;
    throw;
  }
  
  // Update RowGroupInformation and add it to the list
  auto curRowGroupInfo = pixels::fb::CreateRowGroupInformation(
        fbb,
        uploadedFooterOffset,
        rowGroupDataLength,
        static_cast<uint32_t>(footerBufSize),
        curRowGroupNumOfRows,
        0                     );
  rowGroupInfoList.push_back(curRowGroupInfo);

  // store rowgroupInfo
  RowGroupNative nativeInfo;
  nativeInfo.footerOffset = (uint64_t)uploadedFooterOffset;
  nativeInfo.footerLength = (uint32_t)footerBufSize;
  nativeInfo.dataLength = this->curRowGroupDataLength;
  nativeInfo.numberOfRows = this->curRowGroupNumOfRows;
  rowGroupMetadataList.push_back(nativeInfo);
  
  this->fileRowNum += curRowGroupNumOfRows;
  this->fileContentLength += rowGroupDataLength;

  
  // release fbb
  fbb.Clear();
}


void PixelsWriterImpl::writeFileTail()
{
  fbb.Clear();

  std::vector<flatbuffers::Offset<pixels::fb::RowGroupInformation>> infoOffsets;
  for (size_t i = 0; i < rowGroupMetadataList.size(); i++) {
    const auto& info = rowGroupMetadataList[i];
    
    auto fbInfo = pixels::fb::CreateRowGroupInformation(
        fbb,
        info.footerOffset,
        info.dataLength,
        info.footerLength,
        info.numberOfRows
    );
    infoOffsets.push_back(fbInfo);
  }
  auto rowGroupInfoVector = fbb.CreateVector(infoOffsets);

  // === 1. build Footer ===
  size_t footerStart = fbb.GetSize();

  auto typeOffsets = schema->writeTypes(fbb);
  auto typeVector = fbb.CreateVector(typeOffsets);
  auto footerOffset = pixels::fb::CreateFooter(fbb, typeVector, 0, rowGroupInfoVector);

  uint32_t footerLength = static_cast<uint32_t>(fbb.GetSize() - footerStart);

  // === 2. build PostScript ===
  size_t psStart = fbb.GetSize();

  auto magicOffset = fbb.CreateString("PIXELS");
  auto postScriptOffset = pixels::fb::CreatePostScript(
      fbb,
      PixelsVersion::V1,
      fileContentLength,
      fileRowNum,
      compressionKind,
      compressionBlockSize,
      columnWriterOption->getPixelsStride(),
      0,  // writerTimeZone
      partitioned,
      CHUNK_ALIGNMENT,
      false,
      magicOffset
  );

  uint32_t postScriptLength = static_cast<uint32_t>(fbb.GetSize() - psStart);

  // === 3. build root object FileTail ===
  auto fileTailOffset = pixels::fb::CreateFileTail(
      fbb,
      footerOffset,
      postScriptOffset,
      footerLength,
      postScriptLength
  );

  fbb.Finish(fileTailOffset);

  // === 4. physical write ===
  uint8_t *bufferPtr = fbb.GetBufferPointer();
  size_t bufferSize = fbb.GetSize();

  int totalFileTailLen = static_cast<int>(bufferSize + 8);
  physicalWriter->prepare(totalFileTailLen);

  long tailOffset = physicalWriter->append(bufferPtr, 0, bufferSize);

  if (Endianness::isLittleEndian())
  {
      tailOffset = (long) __builtin_bswap64(tailOffset);
  }

  std::shared_ptr<ByteBuffer> tailOffsetBuffer = std::make_shared<ByteBuffer>(8);
  tailOffsetBuffer->putLong(tailOffset);
  physicalWriter->append(tailOffsetBuffer);

  writtenBytes += totalFileTailLen;
  physicalWriter->flush();
}
