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
#ifndef PIXELS_PIXELSRECORDREADER_H
#define PIXELS_PIXELSRECORDREADER_H

#include "vector/VectorizedRowBatch.h"
#include "physical/RequestBatch.h"
#include "TypeDescription.h"

class PixelsRecordReader
{
public:
//    virtual int prepareBatch(int batchSize) = 0;
    virtual std::shared_ptr <VectorizedRowBatch> readBatch(bool reuse) = 0;


    /**
     * Get the schema of the included columns in the read option.
     *
     * @return result schema, null if PixelsRecordReader is not initialized successfully.
     */
    virtual std::shared_ptr <TypeDescription> getResultSchema() = 0;

    virtual bool isEndOfFile() = 0;

    virtual void close() = 0;

};
#endif //PIXELS_PIXELSRECORDREADER_H
