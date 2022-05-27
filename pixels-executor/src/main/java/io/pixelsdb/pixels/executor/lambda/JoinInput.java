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
package io.pixelsdb.pixels.executor.lambda;

import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.lambda.ScanInput.OutputInfo;

/**
 * @author hank
 * @date 22/05/2022
 */
public interface JoinInput
{
    public JoinType getJoinType();

    /**
     * Get the alias of the columns in the join result. The order of the alias <b>MUST</b>
     * follow the order of the left table columns and the right table columns.
     * <p/>
     * For example, if the left table L scans 3 column A, B, and C, whereas the right table R
     * scans 4 columns D, E, F, and G. The join condition is (L inner join R on L.A=R.B).
     * Then, the joined columns would be A, B, C, D, E, F, and G. And the alias of the joined
     * columns must follow this order, such as A_0, B_1, C_2, D_3, E_4, F_5, and G_6.
     *
     * @return the alias of the columns in the join result
     */
    public String[] getJoinedCols();

    /**
     * Get the information of the join output.
     * @return the join output information
     */
    public OutputInfo getOutput();
}
