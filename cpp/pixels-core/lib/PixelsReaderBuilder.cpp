//
// Created by liyu on 3/6/23.
//

#include "PixelsReaderBuilder.h"
#include "utils/Endianness.h"

PixelsReaderBuilder::PixelsReaderBuilder() {
    builderPath = "";
	builderPixelsFooterCache = nullptr;
}

PixelsReaderBuilder * PixelsReaderBuilder::setStorage(std::shared_ptr<Storage> storage) {
    builderStorage = storage;
    return this;
}

PixelsReaderBuilder * PixelsReaderBuilder::setPath(const std::string &path) {
    builderPath = path;
    return this;
}


PixelsReaderBuilder * PixelsReaderBuilder::setPixelsFooterCache(std::shared_ptr<PixelsFooterCache> pixelsFooterCache) {
    builderPixelsFooterCache = pixelsFooterCache;
    return this;
}

std::shared_ptr<PixelsReader> PixelsReaderBuilder::build() {
    if(builderStorage.get() == nullptr || builderPath.empty()) {
        throw std::runtime_error("Missing argument to build PixelsReader");
    }
    // get PhysicalReader
    std::shared_ptr<PhysicalReader> fsReader =
	    PhysicalReaderUtil::newPhysicalReader(builderStorage, builderPath);
    // try to get file tail from cache
    std::string fileName = fsReader->getName();
    std::shared_ptr<pixels::proto::FileTail> fileTail;
    if(builderPixelsFooterCache != nullptr && builderPixelsFooterCache->containsFileTail(fileName)) {
        fileTail = builderPixelsFooterCache->getFileTail(fileName);
    } else {
        if(fsReader.get() == nullptr) {
            throw PixelsReaderException(
                    "Failed to create PixelsReader due to error of creating PhysicalReader");
        }
        // get FileTail
        long fileLen = fsReader->getFileLength();
//        std::cout<<"filelen: "<<fsReader->getFileLength()<<std::endl;
        fsReader->seek(fileLen - (long)sizeof(long));
        long fileTailOffset=fsReader->readLong();
        if(Endianness::isLittleEndian()){
          fileTailOffset=(long)__builtin_bswap64(fileTailOffset);
        }
//        std::cout<<"fileTailOffset: "<<fileTailOffset<<std::endl;
        int fileTailLength = (int) (fileLen - fileTailOffset - sizeof(long));
        fsReader->seek(fileTailOffset);
        std::shared_ptr<ByteBuffer> fileTailBuffer = fsReader->readFully(fileTailLength);
		fileTail = std::make_shared<pixels::proto::FileTail>();
        if(!fileTail->ParseFromArray(fileTailBuffer->getPointer(),
                                    fileTailLength)) {
            throw InvalidArgumentException("PixelsReaderBuilder::build: paring FileTail error!");
        }
		if(builderPixelsFooterCache != nullptr) {
			builderPixelsFooterCache->putFileTail(fileName, fileTail);
		}
    }

    // check file MAGIC and file version
    pixels::proto::PostScript postScript = fileTail->postscript();
    uint32_t fileVersion = postScript.version();
    const std::string& fileMagic = postScript.magic();
    if(PixelsVersion::currentVersion() != fileVersion) {
        throw PixelsFileVersionInvalidException(fileVersion);
    }
    if(fileMagic != Constants::MAGIC) {
        throw PixelsFileMagicInvalidException(fileMagic);
    }


	auto fileColTypes = std::vector<std::shared_ptr<pixels::proto::Type>>{};
	for(const auto& type : fileTail->footer().types()) {
		fileColTypes.emplace_back(std::make_shared<pixels::proto::Type>(type));
	}
	builderSchema = TypeDescription::createSchema(fileColTypes);

    // TODO: the remaining things, such as builderSchema, coreCOnfig, metric

	return std::make_shared<PixelsReaderImpl>(builderSchema, fsReader, fileTail,
	                                         builderPixelsFooterCache);
}


