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

//
// Created by gengdy on 24-11-18.
//

#ifndef PIXELS_ENCODINGLEVEL_H
#define PIXELS_ENCODINGLEVEL_H

#include <string>

class EncodingLevel {
public:
    enum Level {
        EL0 = 0,
        EL1 = 1,
        EL2 = 2
    };

    EncodingLevel();
    explicit EncodingLevel(Level level);
    explicit EncodingLevel(int level);
    explicit EncodingLevel(const std::string &level);

    static EncodingLevel from(int level);
    static EncodingLevel from(const std::string &level);

    static bool isValid(int level);

    bool ge(int level) const;
    bool ge(const EncodingLevel &encodingLevel) const;

    bool equals(int level) const;
    bool equals(const EncodingLevel &encodingLevel)const;

private:
    Level level;
};
#endif //PIXELS_ENCODINGLEVEL_H
