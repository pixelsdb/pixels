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
  if (builderStorage.get() == nullptr || builderPath.empty())
  {
    throw std::runtime_error("Missing argument to build PixelsReader");
  }
  // get PhysicalReader
  std::shared_ptr<PhysicalReader> fsReader =
      PhysicalReaderUtil::newPhysicalReader(builderStorage, builderPath);
  // try to get file tail from cache
  std::string fileName = fsReader->getName();
  std::shared_ptr<pixels::proto::FileTail> fileTail;
  if (builderPixelsFooterCache != nullptr && builderPixelsFooterCache->containsFileTail(fileName))
  {
    fileTail = builderPixelsFooterCache->getFileTail(fileName);
  } else
  {
    if (fsReader.get() == nullptr)
    {
      throw PixelsReaderException(
          "Failed to create PixelsReader due to error of creating PhysicalReader");
    }
    // get FileTail
    long fileLen = fsReader->getFileLength();
    fsReader->seek(fileLen - (long) sizeof(long));
    // get FileTailOffset

    long fileTailOffset = fsReader->readLong();
    if (Endianness::isLittleEndian())
    {
      fileTailOffset = (long) __builtin_bswap64(fileTailOffset);
    }

    int fileTailLength = (int) (fileLen - fileTailOffset - sizeof(long));
    fsReader->seek(fileTailOffset);
    std::shared_ptr<ByteBuffer> fileTailBuffer = fsReader->readFully(fileTailLength);
    fileTail = std::make_shared<pixels::proto::FileTail>();
    if (!fileTail->ParseFromArray(fileTailBuffer->getPointer(),
                                  fileTailLength))
    {
      throw InvalidArgumentException("PixelsReaderBuilder::build: paring FileTail error!");
    }
    if (builderPixelsFooterCache != nullptr)
    {
      builderPixelsFooterCache->putFileTail(fileName, fileTail);
    }
  }

  // check file MAGIC and file version
  pixels::proto::PostScript postScript = fileTail->postscript();
  uint32_t fileVersion = postScript.version();
  const std::string &fileMagic = postScript.magic();
  if (PixelsVersion::currentVersion() != fileVersion)
  {
    throw PixelsFileVersionInvalidException(fileVersion);
  }
  if (fileMagic != Constants::MAGIC)
  {
    throw PixelsFileMagicInvalidException(fileMagic);
  }

  auto fileColTypes = std::vector<std::shared_ptr<pixels::proto::Type >>{};
  for (const auto &type : fileTail->footer().types())
  {
    fileColTypes.emplace_back(std::make_shared<pixels::proto::Type>(type));
  }
  builderSchema = TypeDescription::createSchema(fileColTypes);

  // TODO: the remaining things, such as builderSchema, coreCOnfig, metric

  return std::make_shared<PixelsReaderImpl>(builderSchema, fsReader, fileTail,
                                            builderPixelsFooterCache);
}


