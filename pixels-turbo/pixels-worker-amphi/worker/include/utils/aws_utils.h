#ifndef PIXELS_WORKER_AMPHI_AWS_UTILS_H
#define PIXELS_WORKER_AMPHI_AWS_UTILS_H

#include "aws/core/Aws.h"
#include "aws/s3/S3Client.h"
#include "aws/s3/model/ListBucketsResult.h"
#include "aws/s3/model/ListObjectsRequest.h"

namespace awsutils {
    bool ListBuckets(const Aws::Client::ClientConfiguration &clientConfig);
    bool ListObjects(const Aws::String &bucketName,
                     const Aws::Client::ClientConfiguration &clientConfig);

}
#endif //PIXELS_WORKER_AMPHI_AWS_UTILS_H
