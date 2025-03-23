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
#ifndef EXAMPLE_C_PIXELSREADGLOBALSTATE_HPP
#define EXAMPLE_C_PIXELSREADGLOBALSTATE_HPP

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "PixelsReader.h"
#include "physical/StorageArrayScheduler.h"

namespace duckdb
{

    struct PixelsReadGlobalState : public GlobalTableFunctionState
    {
        mutex lock;

        //! The initial reader from the bind phase
        std::shared_ptr <PixelsReader> initialPixelsReader;

        //! Mutexes to wait for a file that is currently being opened
        unique_ptr<mutex[]> file_mutexes;

        //! Signal to other threads that a file failed to open, letting every thread abort.
        bool error_opening_file = false;

        std::shared_ptr <StorageArrayScheduler> storageArrayScheduler;

        //! Index of file currently up for scanning
        vector <idx_t> file_index;

        //! Batch index of the next row group to be scanned
        idx_t batch_index;

        idx_t max_threads;

        TableFilterSet *filters;

        atomic<idx_t> cur_file_index;

        idx_t MaxThreads() const override
        {
            return max_threads;
        }
    };

}

#endif // EXAMPLE_C_PIXELSREADGLOBALSTATE_HPP
