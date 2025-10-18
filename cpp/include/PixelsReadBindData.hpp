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
 * @create 2023-03-27
 */
#ifndef EXAMPLE_C_PIXELSREADBINDDATA_HPP
#define EXAMPLE_C_PIXELSREADBINDDATA_HPP


#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "PixelsReader.h"


namespace duckdb
{

    struct PixelsReadBindData : public TableFunctionData
    {
        std::shared_ptr <PixelsReader> initialPixelsReader;
        std::shared_ptr <TypeDescription> fileSchema;
        vector <string> files;
        atomic <int> curFileId;
    };

}
#endif // EXAMPLE_C_PIXELSREADBINDDATA_HPP
