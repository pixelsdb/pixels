//
// Created by gengdy on 24-11-19.
//

#include <load/Parameters.h>

Parameters::Parameters(const std::string &schema, int maxRowNum, const std::string &regex,
                       const std::string &loadingPath, EncodingLevel encodingLevel, bool nullsPadding)
                       : schema(schema), maxRowNum(maxRowNum), regex(regex), loadingPath(loadingPath),
                         encodingLevel(encodingLevel), nullsPadding(nullsPadding) {}

std::string Parameters::getSchema() const {
    return this->schema;
}

int Parameters::getMaxRowNum() const {
    return this->maxRowNum;
}

std::string Parameters::getRegex() const {
    return this->regex;
}

std::string Parameters::getLoadingPath() const {
    return this->loadingPath;
}

EncodingLevel Parameters::getEncodingLevel() const {
    return this->encodingLevel;
}

bool Parameters::isNullsPadding() const {
    return this->nullsPadding;
}