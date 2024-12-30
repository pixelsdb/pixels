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
package io.pixelsdb.pixels.planner.plan.physical.input;

import io.pixelsdb.pixels.planner.plan.physical.domain.*;

public class SortedJoinInput extends JoinInput
{
        /**
         * The information of the small sorted table.
         */
        private SortedTableInfo smallTable;
        /**
         * The information of the large sorted table.
         */
        private SortedTableInfo largeTable;
        /**
         * The information of the sorted join.
         */
        private SortedJoinInfo joinInfo;

        /**
         * Default constructor for Jackson.
         */
        public SortedJoinInput() { }

        public SortedJoinInput(long transId, long timestamp, SortedTableInfo smallTable, SortedTableInfo largeTable,
                                    SortedJoinInfo joinInfo, boolean partialAggregationPresent,
                                    PartialAggregationInfo partialAggregationInfo, MultiOutputInfo output)
        {
            super(transId, timestamp, partialAggregationPresent, partialAggregationInfo, output);
            this.smallTable = smallTable;
            this.largeTable = largeTable;
            this.joinInfo = joinInfo;
        }

        public SortedTableInfo getSmallTable()
        {
            return smallTable;
        }

        public void setSmallTable(SortedTableInfo smallTable)
        {
            this.smallTable = smallTable;
        }

        public SortedTableInfo getLargeTable()
        {
            return largeTable;
        }

        public void setLargeTable(SortedTableInfo largeTable)
        {
            this.largeTable = largeTable;
        }

        public SortedJoinInfo getJoinInfo()
        {
            return joinInfo;
        }

        public void setJoinInfo(SortedJoinInfo joinInfo)
        {
            this.joinInfo = joinInfo;
        }
}
