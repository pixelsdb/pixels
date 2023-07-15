//
// Created by liyu on 3/6/23.
//

#ifndef PIXELS_STORAGEFACTORY_H
#define PIXELS_STORAGEFACTORY_H
#include <bits/stdc++.h>
#include "physical/Storage.h"
#include "physical/storage/LocalFS.h"

class StorageFactory {
public:
    static StorageFactory * getInstance();

    std::vector<Storage::Scheme> getEnabledSchemes();
    bool isEnabled(Storage::Scheme scheme);
    void closeAll();
    void reloadAll();
    void reload(Storage::Scheme scheme);
    std::shared_ptr<Storage> getStorage(const std::string& schemeOrPath);
	std::shared_ptr<Storage> getStorage(Storage::Scheme scheme);
private:
    //TODO: logger
	StorageFactory();
    std::unordered_map<Storage::Scheme, std::shared_ptr<Storage>> storageImpls;
    std::set<Storage::Scheme> enabledSchemes;
    static StorageFactory * instance;

};
#endif //PIXELS_STORAGEFACTORY_H
