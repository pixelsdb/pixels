/*
 * Copyright 2024 PixelsDB.
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
 * @author gengdy
 * @create 2024-11-19
 */
#ifndef PIXELS_PARAMETERS_H
#define PIXELS_PARAMETERS_H

#include <string>
#include <encoding/EncodingLevel.h>

class Parameters
{
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
