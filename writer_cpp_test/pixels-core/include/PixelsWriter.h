//
// Created by gengdy on 24-11-25.
//

#ifndef PIXELS_PIXELSWRITER_H
#define PIXELS_PIXELSWRITER_H

#include "TypeDescription.h"

class PixelsWriter {
public:

    virtual ~PixelsWriter() = default;
    /**
     * Add row batch into the file that is not hash partitioned.
     *
     * @param rowBatch the row batch to be written.
     * @return if the file adds a new row group, returns false. Otherwise, returns true.
     */
    virtual bool addRowBatch(std::shared_ptr<VectorizedRowBatch> rowBatch) = 0;

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
