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
 * @create 2023-03-01
 */
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
