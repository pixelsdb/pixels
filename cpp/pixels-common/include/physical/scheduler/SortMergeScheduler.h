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
 * @create 2023-05-01
 */
#ifndef DUCKDB_SORTMERGESCHEDULER_H
#define DUCKDB_SORTMERGESCHEDULER_H

#include "physical/Scheduler.h"
#include "physical/MergedRequest.h"
#include<algorithm>
#include "exception/InvalidArgumentException.h"

class SortMergeScheduler : public Scheduler {
    // TODO: logger
public:
    static Scheduler * Instance();
	std::vector<std::shared_ptr<MergedRequest>> sortMerge(RequestBatch batch, long queryId);
	std::vector<std::shared_ptr<ByteBuffer>> executeBatch(std::shared_ptr<PhysicalReader> reader,
	                                                                          RequestBatch batch, long queryId) override;
	std::vector<std::shared_ptr<ByteBuffer>> executeBatch(std::shared_ptr<PhysicalReader> reader, RequestBatch batch,
	                                                      std::vector<std::shared_ptr<ByteBuffer>> reuseBuffers, long queryId) override;


private:
    SortMergeScheduler();
    static Scheduler * instance;


};

#endif //DUCKDB_SORTMERGESCHEDULER_H
