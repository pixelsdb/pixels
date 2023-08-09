//
// Created by liyu on 3/6/23.
//

#ifndef PIXELS_PIXELSREADERBUILDER_H
#define PIXELS_PIXELSREADERBUILDER_H

#include "PixelsReaderImpl.h"
#include "physical/PhysicalReaderUtil.h"
#include "pixels-common/pixels.pb.h"
#include "PixelsVersion.h"
#include "PixelsFooterCache.h"
#include "exception/PixelsReaderException.h"
#include "exception/InvalidArgumentException.h"
#include "exception/PixelsFileVersionInvalidException.h"
#include "exception/PixelsFileMagicInvalidException.h"
#include "utils/Constants.h"
#include "TypeDescription.h"

class PixelsReaderBuilder {
public:
    PixelsReaderBuilder();
	PixelsReaderBuilder * setStorage(std::shared_ptr<Storage> storage);
	PixelsReaderBuilder * setPath(const std::string & path);
	PixelsReaderBuilder * setPixelsFooterCache(std::shared_ptr<PixelsFooterCache> pixelsFooterCache);
	std::shared_ptr<PixelsReader> build();

private:
    std::shared_ptr<Storage> builderStorage;
    std::string builderPath;
	std::shared_ptr<PixelsFooterCache> builderPixelsFooterCache;
	std::shared_ptr<TypeDescription> builderSchema;
};
#endif //PIXELS_PIXELSREADERBUILDER_H
