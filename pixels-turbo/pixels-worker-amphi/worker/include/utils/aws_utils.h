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
#ifndef PIXELS_WORKER_AMPHI_AWS_UTILS_H
#define PIXELS_WORKER_AMPHI_AWS_UTILS_H

#include <fstream>
#include <iostream>
#include <filesystem>

#include "aws/core/Aws.h"
#include "aws/s3/S3Client.h"
#include "aws/s3/model/ListBucketsResult.h"
#include "aws/s3/model/ListObjectsRequest.h"
#include "aws/s3/model/GetObjectRequest.h"

namespace awsutils {
    bool ListBuckets(const Aws::Client::ClientConfiguration &clientConfig);
    bool ListObjects(const Aws::String &bucketName,
                     const Aws::Client::ClientConfiguration &clientConfig);
    bool GetObject(const Aws::String &objectKey,
                   const Aws::String &fromBucket,
                   const std::string &store_fp,
                   const Aws::Client::ClientConfiguration &clientConfig);
}
#endif //PIXELS_WORKER_AMPHI_AWS_UTILS_H
