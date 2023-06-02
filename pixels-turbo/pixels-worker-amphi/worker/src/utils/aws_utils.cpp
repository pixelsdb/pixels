#include "utils/aws_utils.h"

bool awsutils::ListBuckets(const Aws::Client::ClientConfiguration &clientConfig) {
    Aws::S3::S3Client client(clientConfig);

    auto outcome = client.ListBuckets();

    bool result = true;
    if (!outcome.IsSuccess()) {
        std::cerr << "Failed with error: " << outcome.GetError() << std::endl;
        result = false;
    }
    else {
        std::cout << "Found " << outcome.GetResult().GetBuckets().size() << " buckets\n";
        for (auto &&b: outcome.GetResult().GetBuckets()) {
            std::cout << b.GetName() << std::endl;
        }
    }

    return result;
}

bool awsutils::ListObjects(const Aws::String &bucketName,
                           const Aws::Client::ClientConfiguration &clientConfig) {
    Aws::S3::S3Client s3_client(clientConfig);

    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucketName);

    auto outcome = s3_client.ListObjects(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: ListObjects: " <<
                  outcome.GetError().GetMessage() << std::endl;
    }
    else {
        Aws::Vector<Aws::S3::Model::Object> objects =
                outcome.GetResult().GetContents();

        for (Aws::S3::Model::Object &object: objects) {
            std::cout << object.GetKey() << std::endl;
        }
    }

    return outcome.IsSuccess();
}

bool awsutils::GetObject(const Aws::String &objectKey,
                         const Aws::String &fromBucket,
                         const std::string &store_fp,
                         const Aws::Client::ClientConfiguration &clientConfig) {
    Aws::S3::S3Client client(clientConfig);

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(fromBucket);
    request.SetKey(objectKey);

    Aws::S3::Model::GetObjectOutcome outcome =
            client.GetObject(request);

    if (!outcome.IsSuccess()) {
        const Aws::S3::S3Error &err = outcome.GetError();
        std::cerr << "Error: GetObject: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        return false;
    }
    else {
        std::filesystem::path dir_path(store_fp + "/" + fromBucket.c_str());
        std::filesystem::path object_path(objectKey.c_str());
        std::filesystem::path full_path = dir_path / object_path;

        std::filesystem::create_directories(full_path.parent_path());

        Aws::OFStream local_file;
        local_file.open(full_path, std::ios::out | std::ios::binary);
        local_file << outcome.GetResult().GetBody().rdbuf();

        std::cout << "Successfully retrieved '" << objectKey << "' from '"
                  << fromBucket << "' and stored to '" << full_path << "'." << std::endl;
    }

    return true;
}
