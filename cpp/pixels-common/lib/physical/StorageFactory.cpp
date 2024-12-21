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
#include "physical/StorageFactory.h"

StorageFactory * StorageFactory::instance = nullptr;

StorageFactory::StorageFactory() {
    //TODO: read enabled.storage.schemes from pixels.properties
    enabledSchemes.insert(Storage::file);
}

StorageFactory * StorageFactory::getInstance() {
    if(instance == nullptr) {
		instance = new StorageFactory();
        //TODO: close all here. Need a ShutdownHook. C++ doesn't have it.
    }
    return instance;
}

std::vector<Storage::Scheme> StorageFactory::getEnabledSchemes() {
    std::vector<Storage::Scheme> schemeVector(enabledSchemes.size());
    std::copy(enabledSchemes.begin(), enabledSchemes.end(), schemeVector.begin());
    return schemeVector;
}

bool StorageFactory::isEnabled(Storage::Scheme scheme) {
    return enabledSchemes.find(scheme) != enabledSchemes.end();
}

void StorageFactory::closeAll() {
    for(auto storageImpl : storageImpls) {
        std::shared_ptr<Storage> storage = storageImpl.second;
        storage->close();
    }
}

std::shared_ptr<Storage> StorageFactory::getStorage(Storage::Scheme scheme) {
    // TODO: make it synchronized
    if(enabledSchemes.find(scheme) == enabledSchemes.end()) {
        throw std::runtime_error("storage scheme is not enabled.");
    }
    if(storageImpls.count(scheme)) {
        return storageImpls[scheme];
    }
	std::shared_ptr<Storage> storage;
    switch (scheme) {
        case Storage::hdfs:
            throw std::runtime_error("hdfs not support");
            break;
        case Storage::file:
            storage = std::make_shared<LocalFS>();
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
    return storage;
}

/**
 * Get the storage instance from a scheme name or a scheme prefixed path.
 * @param schemeOrPath
 * @return
 */
std::shared_ptr<Storage> StorageFactory::getStorage(const std::string& schemeOrPath) {
    // TODO: this function should be synchronized
    try {
        if(schemeOrPath.find("://", 0) != std::string::npos) {
            return getStorage(Storage::fromPath(schemeOrPath));
        } else {
            return getStorage(Storage::from(schemeOrPath));
        }
    } catch(const std::runtime_error& e) {
        throw std::runtime_error("Invalid storage scheme or path: " + schemeOrPath);
    }
}

/**
 * Recreate all the enabled Storage instances.
 * <b>Be careful:</b> all the Storage enabled Storage must be configured well before
 * calling this method. It is better to call {@link #reload(Storage.Scheme)} to reload
 * the Storage that you are sure it is configured or does not need any dynamic configuration.
 *
 * @throws IOException
 */
void StorageFactory::reloadAll() {
    // TODO: this function should be synchronized
    for(Storage::Scheme scheme: enabledSchemes) {
        reload(scheme);
    }
}

/**
 * Recreate the Storage instance for the given storage scheme.
 *
 * @param scheme the given storage scheme
 * @throws IOException
 */
void StorageFactory::reload(Storage::Scheme scheme) {
    // TODO: this function should be synchronized
	std::shared_ptr<Storage> storage;
    if(storageImpls.find(scheme) != storageImpls.end()) {
        storage = storageImpls[scheme];
        storageImpls.erase(scheme);
        storage->close();
    }
    storage = getStorage(scheme);
    storageImpls[scheme] = storage;
}




