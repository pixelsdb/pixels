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
#include "physical/Request.h"


Request::Request(uint64_t queryId_, uint64_t start_, uint64_t length_, int64_t bufferId) {
    queryId = queryId_;
    start = start_;
    length = length_;
    this->bufferId = bufferId;
}

int Request::hashCode() {
    return (int) ((start << 32) >> 32);
}

int Request::comparedTo(Request o) {
    return start == o.start;
}
