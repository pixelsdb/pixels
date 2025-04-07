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
#include "physical/PhysicalWriterUtil.h"
#include "pixels-common/pixels.pb.h"
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
          compressionBlockSize(compressionBlockSize)
{
    this->columnWriterOption = std::make_shared<PixelsWriterOption>()
            ->setPixelsStride(pixelsStride)
            ->setEncodingLevel(encodingLevel)
            ->setNullsPadding(nullsPadding);
    this->physicalWriter = PhysicalWriterUtil::newPhysicalWriter(
            targetFilePath, blockSize, blockPadding, false);
    this->compressionKind = pixels::proto::CompressionKind::NONE;
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
    std::cout << "PixelsWriterImpl::addRowBatch" << std::endl;
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

void PixelsWriterImpl::writeColumnVectors(
        std::vector<std::shared_ptr<ColumnVector>> &columnVectors,
        int rowBatchSize)
{
    std::vector<std::future<void>> futures;
    std::atomic<int> dataLength(0);
    int commonColumnLength = columnVectors.size();

    // Writing regular columns
    for (int i = 0; i < commonColumnLength; ++i)
    {
        // dataLength += columnWriters[i]->write(columnVectors[i], rowBatchSize);
        futures.emplace_back(std::async(std::launch::async, [this, columnVectors,
                rowBatchSize, i,
                &dataLength]()
        {
            try
            {
                dataLength += columnWriters[i]->write(columnVectors[i], rowBatchSize);
            } catch (const std::exception &e)
            {
                throw std::runtime_error("failed to write column vector: " +
                                         std::string(e.what()));
            }
        }));
    }

    // Wait for all futures to complete
    for (auto &future: futures)
    {
        future.get(); // Blocking until all tasks are completed
    }

    // Simulate curRowGroupDataLength accumulation
    curRowGroupDataLength += dataLength.load();
    std::cout << "Data length written: " << curRowGroupDataLength << std::endl;
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
        for (auto cw: columnWriters)
        {
            cw->close();
        }
    } catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        throw;
    }
}

void PixelsWriterImpl::writeRowGroup()
{
    // TODO
    std::cout << "Try to write rowGroup" << std::endl;
    int rowGroupDataLength = 0;
    //    pixels::proto::RowGroupStatistic curRowGroupStatistic;
    pixels::proto::RowGroupInformation curRowGroupInfo;
    pixels::proto::RowGroupIndex curRowGroupIndex;
    pixels::proto::RowGroupEncoding curRowGroupEncoding;
    // reset each column writer and get current row group content size in bytes
    for (auto writer: columnWriters)
    {
        // flush writes the isNull bit map into the internal output stream.
        writer->flush();
        rowGroupDataLength += writer->getColumnChunkSize();
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
    // write and flush row group content
    try
    {
        curRowGroupOffset = physicalWriter->prepare(rowGroupDataLength);
        if (curRowGroupOffset != -1)
        {
            int tryAlign = 0;
            while (CHUNK_ALIGNMENT != 0 && curRowGroupOffset % CHUNK_ALIGNMENT != 0 &&
                   tryAlign++ < 2)
            {
                int alignBytes = CHUNK_ALIGNMENT - curRowGroupOffset % CHUNK_ALIGNMENT;
                physicalWriter->append(CHUNK_PADDING_BUFFER.data(), 0, alignBytes);
                writtenBytes += alignBytes;
                curRowGroupOffset = physicalWriter->prepare(rowGroupDataLength);
            }
            if (tryAlign > 2)
            {
                std::cerr << "Failed to align the start offset of the column chunks in "
                             "the row group"
                          << std::endl;
                throw std::runtime_error("Failed to align the start offset of the "
                                         "column chunks in the row group");
            }

            for (auto &writer: columnWriters)
            {
                auto rowGroupBuffer = writer->getColumnChunkContent();
                physicalWriter->append(rowGroupBuffer.data(), 0, rowGroupBuffer.size());
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
    } catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        throw;
    }

    // update index and stats(necessary?)
    rowGroupDataLength = 0;
    for (int i = 0; i < columnWriters.size(); i++)
    {
        std::shared_ptr<ColumnWriter> writer = columnWriters[i];
        auto chunkIndex = writer->getColumnChunkIndex();
        chunkIndex.set_chunkoffset(curRowGroupOffset + rowGroupDataLength);
        chunkIndex.set_chunklength(writer->getColumnChunkSize());
        chunkIndex.set_littleendian(true);
        rowGroupDataLength += writer->getColumnChunkSize();
        if (CHUNK_ALIGNMENT != 0 && rowGroupDataLength % CHUNK_ALIGNMENT != 0)
        {
            rowGroupDataLength +=
                    CHUNK_ALIGNMENT - rowGroupDataLength % CHUNK_ALIGNMENT;
        }
        *(curRowGroupIndex.add_columnchunkindexentries()) = chunkIndex;
        *(curRowGroupEncoding.add_columnchunkencodings()) =
                writer->getColumnChunkEncoding();

        columnWriters[i] = ColumnWriterBuilder::newColumnWriter(children.at(i),
                                                                columnWriterOption);
    }

    // put curRowGroupIndex into rowGroupFooter
    std::shared_ptr<pixels::proto::RowGroupFooter> rowGroupFooter =
            std::make_shared<pixels::proto::RowGroupFooter>();

    rowGroupFooter->mutable_rowgroupindexentry()->CopyFrom(curRowGroupIndex);
    rowGroupFooter->mutable_rowgroupencoding()->CopyFrom(curRowGroupEncoding);
    std::cout << "curRowGroupEncoding: " << curRowGroupEncoding.ByteSizeLong()
              << std::endl;
    std::cout << "curRowGroupEncoding: " << curRowGroupEncoding.ByteSizeLong()
              << std::endl;
    try
    {
        ByteBuffer footerBuffer(rowGroupFooter->ByteSizeLong());
        rowGroupFooter->SerializeToArray(footerBuffer.getPointer(),
                                         rowGroupFooter->ByteSizeLong());
        physicalWriter->prepare(footerBuffer.size());
        curRowGroupFooterOffset = physicalWriter->append(footerBuffer.getPointer(),
                                                         0, footerBuffer.size());
        writtenBytes += footerBuffer.size();
        physicalWriter->flush();
    } catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        throw;
    }
    // Update RowGroupInformation and add it to the list
    curRowGroupInfo.set_footeroffset(curRowGroupFooterOffset);
    curRowGroupInfo.set_datalength(rowGroupDataLength);
    curRowGroupInfo.set_footerlength(rowGroupFooter->ByteSizeLong());
    curRowGroupInfo.set_numberofrows(curRowGroupNumOfRows);
    rowGroupInfoList.push_back(curRowGroupInfo);

    this->fileRowNum += curRowGroupNumOfRows;
    this->fileContentLength += rowGroupDataLength;
    std::cout << "PixelsWriterImpl::writeRowGroup" << std::endl;
}

void PixelsWriterImpl::writeFileTail()
{
    std::shared_ptr<pixels::proto::Footer> footer =
            std::make_shared<pixels::proto::Footer>();
    std::shared_ptr<pixels::proto::PostScript> postScript =
            std::make_shared<pixels::proto::PostScript>();
    schema->writeTypes(footer);
    for (auto rowGroupInformation: rowGroupInfoList)
    {
        *(footer->add_rowgroupinfos()) = rowGroupInformation;
    }
    postScript->set_version(PixelsVersion::V1);
    std::string FILE_MAGIC = "PIXELS";
    postScript->set_contentlength(fileContentLength);
    postScript->set_numberofrows(fileRowNum);
    postScript->set_compression(compressionKind);
    postScript->set_compressionblocksize(compressionBlockSize);
    postScript->set_pixelstride(columnWriterOption->getPixelsStride());
    postScript->set_partitioned(partitioned);
    postScript->set_columnchunkalignment(CHUNK_ALIGNMENT);
    postScript->set_magic(FILE_MAGIC);

    // build fileTail
    pixels::proto::FileTail fileTail;
    *fileTail.mutable_footer() = *footer;
    *fileTail.mutable_postscript() = *postScript;
    fileTail.set_footerlength(footer->ByteSizeLong());
    fileTail.set_postscriptlength(postScript->ByteSizeLong());

    // flush filetail
    int fileTailLen = fileTail.ByteSizeLong() + 8;
    physicalWriter->prepare(fileTailLen);
    std::shared_ptr<ByteBuffer> fileTailBuffer =
            std::make_shared<ByteBuffer>(fileTail.ByteSizeLong());
    fileTail.SerializeToArray(fileTailBuffer->getPointer(),
                              fileTail.ByteSizeLong());
    long tailOffset = physicalWriter->append(fileTailBuffer->getPointer(), 0,
            fileTail.ByteSizeLong());

    if (Endianness::isLittleEndian())
    {
        tailOffset = (long) __builtin_bswap64(tailOffset);
    }

    std::shared_ptr<ByteBuffer> tailOffsetBuffer =
            std::make_shared<ByteBuffer>(8);
    tailOffsetBuffer->putLong(tailOffset);
    physicalWriter->append(tailOffsetBuffer);
    writtenBytes += fileTailLen;
    physicalWriter->flush();

    std::cout << "PixelsWriterImpl::writeFileTail" << std::endl;
}