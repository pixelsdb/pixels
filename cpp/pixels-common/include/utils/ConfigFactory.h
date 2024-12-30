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
 * @create 2023-04-19
 */
#ifndef DUCKDB_CONFIGFACTORY_H
#define DUCKDB_CONFIGFACTORY_H

#include <iostream>
#include <memory>
#include <map>
#include <string>
#include <cstdlib>
#include "exception/InvalidArgumentException.h"
#include <fstream>
#include <sstream>

class ConfigFactory
{
public:
    static ConfigFactory &Instance();

    void Print();

    std::string getProperty(std::string key);

    bool boolCheckProperty(std::string key);

    std::string getPixelsDirectory();

    std::string getPixelsSourceDirectory();

private:
    ConfigFactory();

    std::map <std::string, std::string> prop;
    std::string pixelsHome;
    std::string pixelsSrc;
};
#endif // DUCKDB_CONFIGFACTORY_H
