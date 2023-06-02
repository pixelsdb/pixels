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
