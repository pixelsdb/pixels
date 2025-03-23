//
// Created by yuly on 19.04.23.
//

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

class ConfigFactory {
public:
	static ConfigFactory & Instance();
	void Print();
	std::string getProperty(std::string key);
    bool boolCheckProperty(std::string key);
	std::string getPixelsDirectory();
    std::string getPixelsSourceDirectory();
private:
	ConfigFactory();
	std::map<std::string, std::string> prop;
	std::string pixelsHome;
    std::string pixelsSrc;
};
#endif // DUCKDB_CONFIGFACTORY_H
