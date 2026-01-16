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
#include "PixelsReaderBuilder.h"
#include "utils/Endianness.h"

PixelsReaderBuilder::PixelsReaderBuilder()
{
    builderPath = "";
    builderPixelsFooterCache = nullptr;
}

PixelsReaderBuilder *PixelsReaderBuilder::setStorage(std::shared_ptr<Storage> storage)
{
    builderStorage = storage;
    return this;
}

PixelsReaderBuilder *PixelsReaderBuilder::setPath(const std::string &path)
{
    builderPath = path;
    return this;
}

PixelsReaderBuilder *PixelsReaderBuilder::setPixelsFooterCache(std::shared_ptr<PixelsFooterCache> pixelsFooterCache)
{
    builderPixelsFooterCache = pixelsFooterCache;
    return this;
}

std::shared_ptr<PixelsReader> PixelsReaderBuilder::build()
{
    if (builderStorage.get () == nullptr || builderPath.empty ())
    {
        throw std::runtime_error ("Missing argument to build PixelsReader");
    }
    // get PhysicalReader
    std::shared_ptr<PhysicalReader> fsReader =
            PhysicalReaderUtil::newPhysicalReader (builderStorage, builderPath);
    // try to get file tail from cache
    std::string fileName = fsReader->getName ();
    const pixels::fb::FileTail* fileTail;
    if (builderPixelsFooterCache != nullptr && builderPixelsFooterCache->containsFileTail (fileName))
    {
        fileTail = builderPixelsFooterCache->getFileTail (fileName);
    } else
    {
        if (fsReader.get () == nullptr)
        {
            throw PixelsReaderException (
                    "Failed to create PixelsReader due to error of creating PhysicalReader");
        }
        // get FileTail
        long fileLen = fsReader->getFileLength ();
        fsReader->seek (fileLen - (long) sizeof (long));
        // get FileTailOffset

        long fileTailOffset = fsReader->readLong ();
        if (Endianness::isLittleEndian ())
        {
            fileTailOffset = (long) __builtin_bswap64 (fileTailOffset);
        }

        int fileTailLength = (int) (fileLen - fileTailOffset - sizeof (long));
        fsReader->seek (fileTailOffset);
        std::shared_ptr<ByteBuffer> fileTailBuffer = fsReader->readFully (fileTailLength);
        fileTail = pixels::fb::GetFileTail(fileTailBuffer->getPointer());
        if (fileTail == nullptr)
        {
            throw InvalidArgumentException ("PixelsReaderBuilder::build: parsing FileTail error!");
        }
        if (builderPixelsFooterCache != nullptr)
        {
            builderPixelsFooterCache->putFileTail (fileName, fileTail);
        }
    }

    // check file MAGIC and file version
    const pixels::fb::PostScript* postScript = fileTail->postscript();
    uint32_t fileVersion = postScript->version();
    const std::string fileMagic = postScript->magic()->str();
    if (PixelsVersion::currentVersion () != fileVersion)
    {
        throw PixelsFileVersionInvalidException (fileVersion);
    }
    if (fileMagic != Constants::MAGIC)
    {
        throw PixelsFileMagicInvalidException (fileMagic);
    }

    auto fileColTypes = std::vector<const pixels::fb::Type*>{};
    for (int i = 0; i < fileTail->footer()->types()->size(); i++)
    {
        fileColTypes.emplace_back(fileTail->footer()->types()->Get(i));
    }
    builderSchema = TypeDescription::createSchema (fileColTypes);

    // TODO: the remaining things, such as builderSchema, coreCOnfig, metric

    return std::make_shared<PixelsReaderImpl> (builderSchema, fsReader, fileTail,
                                               builderPixelsFooterCache);
}
