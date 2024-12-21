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
 * @create 2023-03-06
 */
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
