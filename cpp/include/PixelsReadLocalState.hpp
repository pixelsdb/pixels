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
 * @create 2023-03-26
 */
#ifndef EXAMPLE_C_PIXELSREADLOCALSTATE_HPP
#define EXAMPLE_C_PIXELSREADLOCALSTATE_HPP

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "PixelsReader.h"
#include "reader/PixelsRecordReader.h"

namespace duckdb
{

    struct PixelsReadLocalState : public LocalTableFunctionState
    {
        PixelsReadLocalState()
        {
            curr_file_index = 0;
            next_file_index = 0;
            curr_batch_index = 0;
            next_batch_index = 0;
            rowOffset = 0;
            currPixelsRecordReader = nullptr;
            nextPixelsRecordReader = nullptr;
            vectorizedRowBatch = nullptr;
            currReader = nullptr;
            nextReader = nullptr;
        }

        std::shared_ptr <PixelsRecordReader> currPixelsRecordReader;
        std::shared_ptr <PixelsRecordReader> nextPixelsRecordReader;
        // this is used for storing row batch results.
        std::shared_ptr <VectorizedRowBatch> vectorizedRowBatch;
        int deviceID;
        int rowOffset;
        vector <column_t> column_ids;
        vector <string> column_names;
        std::shared_ptr <PixelsReader> currReader;
        std::shared_ptr <PixelsReader> nextReader;
        idx_t curr_file_index;
        idx_t next_file_index;
        idx_t curr_batch_index;
        idx_t next_batch_index;
        std::string next_file_name;
        std::string curr_file_name;
    };

}

#endif // EXAMPLE_C_PIXELSREADLOCALSTATE_HPP
