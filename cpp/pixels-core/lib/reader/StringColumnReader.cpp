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

/*
 * @author liyu
 * @create 2023-03-20
 */
#include "reader/StringColumnReader.h"
#include "profiler/CountProfiler.h"
#include "vector/BinaryColumnVector.h"

StringColumnReader::StringColumnReader(std::shared_ptr <TypeDescription> type) : ColumnReader(type)
{
    bufferOffset = 0;
    dictStarts = nullptr;
    currentStart = 0;
    nextStart = 0;
    contentBuf = nullptr;
    dictContentBuf = nullptr;
    startsBuf = nullptr;
    contentDecoder = nullptr;
    dictContentOffset = 0;
    dictStartsOffset = 0;
    dictStarts = nullptr;
    startsLength = 0;
}

void StringColumnReader::close()
{

}

void StringColumnReader::read(std::shared_ptr <ByteBuffer> input, const pixels::fb::ColumnEncoding* encoding, int offset,
                              int size, int pixelStride, int vectorIndex, std::shared_ptr <ColumnVector> vector,
                              const pixels::fb::ColumnChunkIndex* chunkIndex, std::shared_ptr <PixelsBitMask> filterMask)
{
    // TODO: support dictionary
    std::shared_ptr <BinaryColumnVector> columnVector =
            std::static_pointer_cast<BinaryColumnVector>(vector);

    if (offset == 0)
    {
        elementIndex = 0;
        bufferOffset = 0;
        isNullOffset = chunkIndex->isNullOffset();
        readContent(input, input->bytesRemaining(), encoding);
    }

    int pixelId = elementIndex / pixelStride;
    bool hasNull = chunkIndex->pixelStatistics()->Get(pixelId)->statistic()->hasNull();
    setValid(input, pixelStride, vector, pixelId, hasNull);

    // TODO: if dictionary encoded
    if (encoding->kind() == pixels::fb::EncodingKind_DICTIONARY)
    {
        bool cascadeRLE = false;
        if (encoding->cascadeEncoding() != nullptr &&
            encoding->cascadeEncoding()->kind() == pixels::fb::EncodingKind_RUNLENGTH)
        {
            cascadeRLE = true;
        }

        for (int i = 0; i < size; i++)
        {
            bool valid = vector->checkValid(i);
            if (elementIndex % pixelStride == 0)
            {
                int pixelId = elementIndex / pixelStride;
                // TODO: should write the remaining code
            }
            if (vector->checkValid(i) && (filterMask == nullptr || filterMask->get(i)))
            {
                int originId = cascadeRLE ? (int) contentDecoder->next() : contentBuf->getInt();
                int tmpLen = dictStarts[originId + 1] - dictStarts[originId];
                // use setRef instead of setVal to reduce memory copy.
                columnVector->setRef(i + vectorIndex, dictContentBuf->getPointer(), dictStarts[originId], tmpLen);
            }
            else if (!valid && (!cascadeRLE) && chunkIndex->nullsPadding())
            {
                // is null: skip this number
                contentBuf->getInt();
            }
            else
            {
                // filter out: skip this number
                if (!cascadeRLE)
                {
                    contentBuf->getInt();
                }
                else
                {
                    contentDecoder->next();
                }
            }
            elementIndex++;
        }
    }
    else
    {
        for (int i = 0; i < size; i++)
        {
            if (elementIndex % pixelStride == 0)
            {
                int pixelId = elementIndex / pixelStride;
                // TODO: should write the remaining code
            }
            bool valid = vector->checkValid(i);
            if (valid && (filterMask == nullptr || filterMask->get(i)))
            {
                currentStart = nextStart;
                nextStart = startsBuf->getInt();
                int len = nextStart - currentStart;
                // use setRef instead of setVal to reduce memory copy
                columnVector->setRef(
                        i + vectorIndex, contentBuf->getPointer(), bufferOffset, len);
                bufferOffset += len;
            }
            else if (!valid)
            {
                // is null: skip this number
                currentStart = nextStart;
                nextStart = startsBuf->getInt();
            }
            else
            {
                // filter out: skip this number
                currentStart = nextStart;
                nextStart = startsBuf->getInt();
                int len = nextStart - currentStart;
                bufferOffset += len;
            }
            elementIndex++;
        }
    }
}

void StringColumnReader::readContent(std::shared_ptr <ByteBuffer> input,
                                     uint32_t inputLength,
                                     const pixels::fb::ColumnEncoding* encoding)
{
    if (encoding->kind() == pixels::fb::EncodingKind_DICTIONARY)
    {
        input->markReaderIndex();
        input->skipBytes(inputLength - 2 * sizeof(int));
        dictContentOffset = input->getInt();
        dictStartsOffset = input->getInt();
        input->resetReaderIndex();
        // read buffers
        contentBuf = std::make_shared<ByteBuffer>(*input, 0, dictContentOffset);
        dictContentBuf = std::make_shared<ByteBuffer>(
                *input, dictContentOffset, dictStartsOffset - dictContentOffset);
        int startsBufLength = inputLength - dictStartsOffset - 2 * sizeof(int);
        startsBuf = std::make_shared<ByteBuffer>(
                *input, dictStartsOffset, startsBufLength);
        int bufferStart = 0;

        if (encoding->cascadeEncoding() != nullptr &&
            encoding->cascadeEncoding()->kind() == pixels::fb::EncodingKind_RUNLENGTH)
        {
            std::shared_ptr <RunLenIntDecoder> startsDecoder =
                    std::make_shared<RunLenIntDecoder>(startsBuf, false);
            if (encoding->dictionarySize() != 0)
            {
                startsLength = (int) encoding->dictionarySize() + 1;
                dictStarts = new int[startsLength];
                int i = 0;
                while (startsDecoder->hasNext())
                {
                    dictStarts[i++] = bufferStart + (int) startsDecoder->next();
                }
            }
            else
            {
                throw InvalidArgumentException("StringColumnReader::readContent: dictionary size must be defined.");
            }
            contentDecoder = std::make_shared<RunLenIntDecoder>(contentBuf, false);
        }
        else
        {
            if (startsBufLength % sizeof(int) != 0)
            {
                throw InvalidArgumentException(
                        "StringColumnReader::readContent: the length of the starts array buffer is invalid. ");
            }
            int startsSize = startsBufLength / sizeof(int);
            if (encoding->dictionarySize() != 0 && encoding->dictionarySize() + 1 != startsSize)
            {
                throw new InvalidArgumentException(
                        "the dictionary size is inconsistent with the size of the starts array");
            }
            dictStarts = new int[startsSize];
            for (int i = 0; i < startsSize; ++i)
            {
                dictStarts[i] = bufferStart + startsBuf->getInt();
            }
            contentDecoder = nullptr;
        }
    }
    else
    {
        input->markReaderIndex();
        input->skipBytes(inputLength - sizeof(int));
        int startsOffset = input->getInt();
        input->resetReaderIndex();
        // read strings
        contentBuf = std::make_shared<ByteBuffer>(*input, 0, startsOffset);
        startsBuf = std::make_shared<ByteBuffer>(
                *input, startsOffset, inputLength - sizeof(int) - startsOffset);
        nextStart = startsBuf->getInt(); // read out the first start offset, which is 0
    }
}

StringColumnReader::~StringColumnReader()
{
    if (dictStarts != nullptr)
    {
        delete[] dictStarts;
    }
}
