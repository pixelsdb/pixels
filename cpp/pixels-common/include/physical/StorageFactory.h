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
#ifndef PIXELS_STORAGEFACTORY_H
#define PIXELS_STORAGEFACTORY_H

#include <bits/stdc++.h>
#include "physical/Storage.h"
#include "physical/storage/LocalFS.h"

class StorageFactory
{
public:
    static StorageFactory *getInstance();

    std::vector <Storage::Scheme> getEnabledSchemes();

    bool isEnabled(Storage::Scheme scheme);

    void closeAll();

    void reloadAll();

    void reload(Storage::Scheme scheme);

    std::shared_ptr <Storage> getStorage(const std::string &schemeOrPath);

    std::shared_ptr <Storage> getStorage(Storage::Scheme scheme);

private:
    //TODO: logger
    StorageFactory();

    std::unordered_map <Storage::Scheme, std::shared_ptr<Storage>> storageImpls;
    std::set <Storage::Scheme> enabledSchemes;
    static StorageFactory *instance;

};
#endif //PIXELS_STORAGEFACTORY_H
