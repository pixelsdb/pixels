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
#include "physical/SchedulerFactory.h"

SchedulerFactory *SchedulerFactory::instance = nullptr;

SchedulerFactory *SchedulerFactory::Instance()
{
    if (instance == nullptr)
    {
        instance = new SchedulerFactory();
    }
    return instance;
}

Scheduler *SchedulerFactory::getScheduler()
{
    return scheduler;
}

SchedulerFactory::SchedulerFactory()
{
    // TODO: here we read name from pixels.properties
    std::string name = ConfigFactory::Instance().getProperty("read.request.scheduler");
    std::transform(name.begin(), name.end(), name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (name == "noop")
    {
        scheduler = NoopScheduler::Instance();
    }
    else if (name == "sortmerge")
    {
        scheduler = SortMergeScheduler::Instance();
    }
    else
    {
        throw std::runtime_error("the read request scheduler is not support. ");
    }
}

SchedulerFactory::~SchedulerFactory()
{
    delete instance;
    instance = nullptr;
}
