//
// Created by liyu on 3/20/23.
//

#include "reader/StringColumnReader.h"
#include "profiler/CountProfiler.h"

StringColumnReader::StringColumnReader(std::shared_ptr<TypeDescription> type) : ColumnReader(type) {
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

void StringColumnReader::close() {

}

void StringColumnReader::read(std::shared_ptr<ByteBuffer> input, pixels::proto::ColumnEncoding & encoding, int offset,
                              int size, int pixelStride, int vectorIndex, std::shared_ptr<ColumnVector> vector,
                              pixels::proto::ColumnChunkIndex & chunkIndex, std::shared_ptr<PixelsBitMask> filterMask) {
    // TODO: support dictionary
    std::shared_ptr<BinaryColumnVector> columnVector =
            std::static_pointer_cast<BinaryColumnVector>(vector);

    if(offset == 0) {
        elementIndex = 0;
        bufferOffset = 0;
        isNullOffset = chunkIndex.isnulloffset();
        readContent(input, input->bytesRemaining(), encoding);
    }

    int pixelId = elementIndex / pixelStride;
    bool hasNull = chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
    setValid(input, pixelStride, vector, pixelId, hasNull);

    // TODO: if dictionary encoded
    if (encoding.kind() == pixels::proto::ColumnEncoding_Kind_DICTIONARY) {
        for(int i = 0; i < size; i++) {
            if(elementIndex % pixelStride == 0) {
                int pixelId = elementIndex / pixelStride;
                // TODO: should write the remaining code
            }
            if(vector->checkValid(i) && (filterMask == nullptr || filterMask->get(i))) {
                int originId = (int) contentDecoder->next();
                int tmpLen = dictStarts[originId + 1] - dictStarts[originId];
                // use setRef instead of setVal to reduce memory copy.
                columnVector->setRef(i + vectorIndex, dictContentBuf->getPointer(), dictStarts[originId], tmpLen);
            } else {
                // skip this number
                contentDecoder->next();
            }
            elementIndex++;
        }
    } else {
        for(int i = 0; i < size; i++) {
            if(elementIndex % pixelStride == 0) {
                int pixelId = elementIndex / pixelStride;
                // TODO: should write the remaining code
            }
            bool valid = vector->checkValid(i);
            if(valid && (filterMask == nullptr || filterMask->get(i))) {
                currentStart = nextStart;
                nextStart = startsBuf->getInt();
                int len = nextStart - currentStart;
                // use setRef instead of setVal to reduce memory copy
                columnVector->setRef(
                        i + vectorIndex, contentBuf->getPointer(), bufferOffset, len);
                bufferOffset += len;
            } else if (!valid) {
                // is null: skip this number
                currentStart = nextStart;
                nextStart = startsBuf->getInt();
            } else {
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

void StringColumnReader::readContent(std::shared_ptr<ByteBuffer> input,
                                     uint32_t inputLength,
                                     pixels::proto::ColumnEncoding & encoding) {
    if(encoding.kind() == pixels::proto::ColumnEncoding_Kind_DICTIONARY) {
        input->markReaderIndex();
        input->skipBytes(inputLength - 2 * sizeof(int));
        dictContentOffset = input->getInt();
        dictStartsOffset = input->getInt();
        input->resetReaderIndex();
        // read buffers
        contentBuf = std::make_shared<ByteBuffer>(*input, 0, dictContentOffset);
		dictContentBuf = std::make_shared<ByteBuffer>(
		    *input, dictContentOffset, dictStartsOffset - dictContentOffset);
		startsBuf = std::make_shared<ByteBuffer>(
		    *input, dictStartsOffset, inputLength - dictStartsOffset - 2 * sizeof(int));
		int bufferStart = 0;
		std::shared_ptr<RunLenIntDecoder> startsDecoder =
		    std::make_shared<RunLenIntDecoder>(startsBuf, false);
		if(encoding.has_dictionarysize()) {
			startsLength = (int)encoding.dictionarysize() + 1;
			dictStarts = new int[startsLength];
			int i = 0;
			while (startsDecoder->hasNext()) {
				dictStarts[i++] = bufferStart + (int) startsDecoder->next();
			}
			dictStarts[i] = bufferStart + dictStartsOffset - dictContentOffset;
		} else {
            throw InvalidArgumentException("StringColumnReader::readContent: dictionary size must be defined.");
		}
		contentDecoder = std::make_shared<RunLenIntDecoder>(contentBuf, false);
    } else {
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
StringColumnReader::~StringColumnReader() {
	if(dictStarts != nullptr) {
		delete[] dictStarts;
	}
}
