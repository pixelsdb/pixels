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
 * @create 2024-11-25
 */
#ifndef PIXELS_PIXELSWRITER_H
#define PIXELS_PIXELSWRITER_H

#include "TypeDescription.h"

class PixelsWriter
{
public:

    virtual ~PixelsWriter() = default;

    /**
     * Add row batch into the file that is not hash partitioned.
     *
     * @param rowBatch the row batch to be written.
     * @return if the file adds a new row group, returns false. Otherwise, returns true.
     */
    virtual bool addRowBatch(std::shared_ptr <VectorizedRowBatch> rowBatch) = 0;

    virtual void close() = 0;

//    /**
//     * Get schema of this file.
//     *
//     * @return schema
//     */
//    virtual std::shared_ptr<TypeDescription> getSchema();
//
//    /**
//     * Get the number of row groups that have been written into this file.
//     * @return
//     */
//    virtual int getNumGroup();
//
//    virtual int getNumWriteRequests();
//
//    virtual long getCompletedBytes();
};
#endif //PIXELS_PIXELSWRITER_H
