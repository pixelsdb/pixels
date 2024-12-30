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
 * @create 2024-11-18
 */
#include <encoding/EncodingLevel.h>
#include <stdexcept>

EncodingLevel::EncodingLevel()
{}

EncodingLevel::EncodingLevel(Level level)
{
    this->level = level;
}

EncodingLevel::EncodingLevel(int level)
{
    if (!isValid(level))
    {
        throw std::invalid_argument("invalid encoding level " + std::to_string(level));
    }
    this->level = static_cast<Level>(level);
}

EncodingLevel::EncodingLevel(const std::string &level)
{
    if (level.empty())
    {
        throw std::invalid_argument("level is null");
    }
    if (!isValid(std::stoi(level)))
    {
        throw std::invalid_argument("invalid encoding level " + level);
    }
    this->level = static_cast<Level>(std::stoi(level));
}

EncodingLevel EncodingLevel::from(int level)
{
    return EncodingLevel(level);
}

EncodingLevel EncodingLevel::from(const std::string &level)
{
    return EncodingLevel(level);
}

bool EncodingLevel::isValid(int level)
{
    return level >= 0 && level <= 2;
}

bool EncodingLevel::ge(int level) const
{
    if (!isValid(level))
    {
        throw std::invalid_argument("level is invalid");
    }
    return static_cast<int>(this->level) >= level;
}

bool EncodingLevel::ge(const EncodingLevel &encodingLevel) const
{
    return this->level >= encodingLevel.level;
}

bool EncodingLevel::equals(int level) const
{
    if (!isValid(level))
    {
        throw std::invalid_argument("level is invalid");
    }
    return static_cast<int>(this->level) == level;
}

bool EncodingLevel::equals(const EncodingLevel &encodingLevel) const
{
    return this->level == encodingLevel.level;
}