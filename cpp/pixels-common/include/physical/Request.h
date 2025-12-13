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
 * @create 2023-03-07
 */
#ifndef PIXELS_REQUEST_H
#define PIXELS_REQUEST_H

#include <iostream>

class Request
{
public:
    int64_t bufferId;
    uint64_t queryId;
    uint64_t start;
    uint64_t length;
    int ringIndex;

    Request(uint64_t queryId_, uint64_t start_, uint64_t length_,
            int64_t bufferId = -1);

    int hashCode();

    int comparedTo(Request o);
};
#endif //PIXELS_REQUEST_H
