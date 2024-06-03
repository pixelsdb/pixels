/*
 * Copyright 2017-2019 PixelsDB.
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
#pragma once

/**
 * @author mzp0514
 * @date 27/05/2022
 */
enum class ValueType {
  NONE,  // Useful when the type of column vector has not be determined yet.
  LONG,
  DOUBLE,
  BYTES,
  DECIMAL,
  TIMESTAMP,
  INTERVAL_DAY_TIME,
  STRUCT,
  LIST,
  MAP,
  UNION
};