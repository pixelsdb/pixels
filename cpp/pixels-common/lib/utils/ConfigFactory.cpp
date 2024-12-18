//
// Created by yuly on 19.04.23.
//

#include "utils/ConfigFactory.h"

ConfigFactory & ConfigFactory::Instance() {
	static ConfigFactory instance;
	return instance;
}

ConfigFactory::ConfigFactory() {
    if(std::getenv("PIXELS_SRC") == nullptr) {
        throw InvalidArgumentException("The environment variable 'PIXELS_SRC' is not set. ");
    }
    pixelsSrc = std::string(std::getenv("PIXELS_SRC"));
    std::cout<<"PIXELS_SRC is "<<pixelsSrc<<std::endl;
    if(pixelsSrc.back() != '/') {
        pixelsSrc += "/";
    }

	if(std::getenv("PIXELS_HOME") == nullptr) {
		throw InvalidArgumentException("The environment variable 'PIXELS_HOME' is not set. ");
	}
	pixelsHome = std::string(std::getenv("PIXELS_HOME"));
	std::cout<<"PIXELS_HOME is "<<pixelsHome<<std::endl;
	if(pixelsHome.back() != '/') {
		pixelsHome += "/";
	}
	std::ifstream infile(pixelsHome + "etc/pixels-cpp.properties");
	std::cout<<"pixels properties file is "<<pixelsHome + "etc/pixels-cpp.properties"<<std::endl;
	std::string line;
	while (std::getline(infile, line)) {
		if (line.find('=') != std::string::npos && line.at(0) != '#') {
			std::string key = line.substr(0, line.find('='));
			std::string value = line.substr(line.find('=') + 1, line.size() - line.find('=') - 1);
			prop[key] = value;
		}
	}
}

void ConfigFactory::Print() {
	for(auto kv : prop) {
		std::cout<<kv.first<<" "<<kv.second<<std::endl;
	}
}

std::string ConfigFactory::getProperty(std::string key) {
	if(prop.find(key) == prop.end()) {
		throw InvalidArgumentException("ConfigFactory::getProperty: no key found: " + key);
	}
	return prop[key];
}

bool ConfigFactory::boolCheckProperty(std::string key) {
	if(getProperty(key) == "true") {
		return true;
	} else if (getProperty(key) == "false") {
		return false;
	} else {
		throw InvalidArgumentException("ConfigFactory: The key is not boolean type.");
	}
}

std::string ConfigFactory::getPixelsDirectory() {
	return pixelsHome;
}

std::string ConfigFactory::getPixelsSourceDirectory() {
    return pixelsSrc;
}