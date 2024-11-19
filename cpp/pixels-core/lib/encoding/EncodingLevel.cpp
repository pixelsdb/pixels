//
// Created by gengdy on 24-11-18.
//

#include <encoding/EncodingLevel.h>
#include <stdexcept>

EncodingLevel::EncodingLevel(Level level) {
    this->level = level;
}

EncodingLevel::EncodingLevel(int level) {
    if (!isValid(level)) {
        throw std::invalid_argument("invalid encoding level " + std::to_string(level));
    }
    this->level = static_cast<Level>(level);
}

EncodingLevel::EncodingLevel(const std::string &level) {
    if (level.empty()) {
        throw std::invalid_argument("level is null");
    }
    if (!isValid(std::stoi(level))) {
        throw std::invalid_argument("invalid encoding level " + level);
    }
    this->level = static_cast<Level>(std::stoi(level));
}

EncodingLevel EncodingLevel::from(int level) {
    return EncodingLevel(level);
}

EncodingLevel EncodingLevel::from(const std::string &level) {
    return EncodingLevel(level);
}

bool EncodingLevel::isValid(int level) {
    return level >= 0 && level <= 2;
}

bool EncodingLevel::ge(int level) const {
    if (!isValid(level)) {
        throw std::invalid_argument("level is invalid");
    }
    return static_cast<int>(this->level) >= level;
}

bool EncodingLevel::ge(const EncodingLevel &encodingLevel) const {
    return this->level >= encodingLevel.level;
}

bool EncodingLevel::equals(int level) const {
    if (!isValid(level)) {
        throw std::invalid_argument("level is invalid");
    }
    return static_cast<int>(this->level) == level;
}

bool EncodingLevel::equals(const EncodingLevel &encodingLevel) const {
    return this->level == encodingLevel.level;
}