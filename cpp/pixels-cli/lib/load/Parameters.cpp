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
#include <load/Parameters.h>

Parameters::Parameters(const std::string &schema, int maxRowNum, const std::string &regex,
                       const std::string &loadingPath, EncodingLevel encodingLevel, bool nullsPadding)
        : schema(schema), maxRowNum(maxRowNum), regex(regex), loadingPath(loadingPath),
          encodingLevel(encodingLevel), nullsPadding(nullsPadding)
{}

std::string Parameters::getSchema() const
{
    return this->schema;
}

int Parameters::getMaxRowNum() const
{
    return this->maxRowNum;
}

std::string Parameters::getRegex() const
{
    return this->regex;
}

std::string Parameters::getLoadingPath() const
{
    return this->loadingPath;
}

EncodingLevel Parameters::getEncodingLevel() const
{
    return this->encodingLevel;
}

bool Parameters::isNullsPadding() const
{
    return this->nullsPadding;
}