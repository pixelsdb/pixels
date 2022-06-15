/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.executor.lambda.input;

import io.pixelsdb.pixels.executor.lambda.domain.MultiOutputInfo;

/**
 * @author hank
 * @date 22/05/2022
 */
public interface JoinInput
{
    /**
     * Get the information about the join output.
     * @return the join output information
     */
    public MultiOutputInfo getOutput();

    /**
     * Set the information about the join output.
     * @param output the join output
     */
    void setOutput(MultiOutputInfo output);
}
