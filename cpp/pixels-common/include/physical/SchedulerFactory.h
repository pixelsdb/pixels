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
 * @create 2023-03-10
 */
#ifndef PIXELS_SCHEDULERFACTORY_H
#define PIXELS_SCHEDULERFACTORY_H

#include "physical/Scheduler.h"
#include "physical/scheduler/NoopScheduler.h"
#include "physical/scheduler/SortMergeScheduler.h"
#include "utils/ConfigFactory.h"
#include <algorithm>
#include <cctype>
#include <string>

class SchedulerFactory {
public:
    static SchedulerFactory * Instance();
    Scheduler * getScheduler();
	~SchedulerFactory();
private:
    static SchedulerFactory * instance;
    Scheduler * scheduler;
    SchedulerFactory();
};
#endif //PIXELS_SCHEDULERFACTORY_H
