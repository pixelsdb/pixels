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
#ifndef PIXELS_PHYSICALREADERUTIL_H
#define PIXELS_PHYSICALREADERUTIL_H

#include "io/PhysicalLocalReader.h"
#include "Storage.h"
#include "StorageFactory.h"
#include <memory>

class PhysicalReaderUtil
{
public:
    static std::shared_ptr <PhysicalReader> newPhysicalReader(std::shared_ptr <Storage> storage, std::string path)
    {
        if (storage == nullptr)
        {
            throw std::runtime_error("storage should not be nullptr");
        }
        if (path.size() == 0)
        {
            throw std::runtime_error("path should not be empty");
        }
        std::shared_ptr <PhysicalReader> reader;
        switch (storage->getScheme())
        {
            case Storage::hdfs:
                throw std::runtime_error("hdfs not support");
                break;
            case Storage::file:
                reader = std::make_shared<PhysicalLocalReader>(storage, path);
                break;
            case Storage::s3:
                throw std::runtime_error("hdfs not support");
                break;
            case Storage::minio:
                throw std::runtime_error("hdfs not support");
                break;
            case Storage::redis:
                throw std::runtime_error("hdfs not support");
                break;
            case Storage::gcs:
                throw std::runtime_error("hdfs not support");
                break;
            case Storage::mock:
                throw std::runtime_error("hdfs not support");
                break;
            default:
                throw std::runtime_error("hdfs not support");
        }
        return reader;
    }

    static std::shared_ptr <PhysicalReader> newPhysicalReader(Storage::Scheme scheme, std::string path)
    {
        if (path.size() == 0)
        {
            throw std::runtime_error("path should not be empty");
        }
        return newPhysicalReader(StorageFactory::getInstance()->getStorage(scheme), path);
    }
};
#endif //PIXELS_PHYSICALREADERUTIL_H
