//
// Created by gengdy on 24-11-19.
//

#ifndef PIXELS_PARAMETERS_H
#define PIXELS_PARAMETERS_H

#include <string>
#include <encoding/EncodingLevel.h>

class Parameters {
public:
    Parameters(const std::string &schema, int maxRowNum, const std::string &regex,
               const std::string &loadingPath, EncodingLevel encodingLevel, bool nullsPadding);
    std::string getLoadingPath() const;
    std::string getSchema() const;
    int getMaxRowNum() const;
    std::string getRegex() const;
    EncodingLevel getEncodingLevel() const;
    bool isNullsPadding() const;

private:
    std::string schema;
    int maxRowNum;
    std::string regex;
    std::string loadingPath;
    EncodingLevel encodingLevel;
    bool nullsPadding;
};
#endif //PIXELS_PARAMETERS_H
