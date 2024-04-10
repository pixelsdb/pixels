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
package io.pixelsdb.pixels.common.server;

/**
 * @author hank
 * @create 2023-05-24
 */
public enum ExecutionHint
{
    BEST_OF_EFFORT, // execute the query at best of effort, without any guarantee of timeliness or performance
    RELAXED, // the query can be postponed a few minutes for execution
    IMMEDIATE, // execute the query immediately using Pixel-Turbo
}
