//
// Created by liyu on 2/27/23.
//

#ifndef PIXELS_READER_STORAGE_H
#define PIXELS_READER_STORAGE_H

#include <iostream>
#include <map>
#include <string>
#include <algorithm>
#include <cctype>
#include <vector>
#include <memory>
#include <fstream>
#include <filesystem>
#include <physical/Status.h>


class Storage {
public:
    /**
     * If we want to add more storage schemes here, modify this enum.
     */
    enum Scheme {
        hdfs,  // HDFS
        file,  // local fs
        s3,    // Amazon S3
        minio, // Minio
        redis, // Redis
        gcs,   // google cloud storage
        mock, // mock
    };
    static std::map<std::string, Scheme> schemeMap;
    Storage();
    ~Storage();
    /**
     * Case-insensitive parsing from String name to enum value.
     * @param value the name of storage scheme.
     * @return
     */
    static Scheme from(std::string value);

    /**
     * Parse the scheme from the path which is prefixed with the storage scheme.
     * @param schemedPath
     */
    static Scheme fromPath(const std::string& schemedPath);

    /**
     * Whether the value is a valid storage scheme.
     * @param value
     * @return
     */
    static bool isValid(const std::string& value);

    // TODO: if we need to implement the function "public boolean equals()" ?

    virtual Scheme getScheme() = 0;

    virtual std::string ensureSchemePrefix(const std::string &path) const = 0;

    virtual std::vector<std::string> listPaths(const std::string &path) = 0;

    virtual std::ifstream open(const std::string &path) = 0;

    virtual void close() = 0;
    // TODO: the remaining function to be implemented
};



#endif //PIXELS_READER_STORAGE_H
