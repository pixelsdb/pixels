//
// Created by liyu on 3/1/23.
//
#include "physical/Storage.h"

std::map<std::string, Storage::Scheme> Storage::schemeMap = {
    {"hdfs", Storage::hdfs},
    {"file", Storage::file},
    {"s3", Storage::s3},
    {"minio", Storage::minio},
    {"redis", Storage::redis},
    {"gcs", Storage::gcs},
    {"mock", Storage::mock},
};

Storage::Storage() {

}

Storage::Scheme Storage::from(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    return Storage::schemeMap[value];
}

Storage::Scheme Storage::fromPath(const std::string& schemedPath) {
    std::size_t separatorIdx = schemedPath.find("://");
    if (separatorIdx != std::string::npos) {
        std::string scheme = schemedPath.substr(0, separatorIdx);
        return from(scheme);
    } else {
        throw std::invalid_argument("Error: schemedPath doesn't contain separator.");
    }
}

bool Storage::isValid(const std::string& value) {
    return schemeMap.find(value) != schemeMap.end();
}

Storage::~Storage() {

}
