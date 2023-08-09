//
// Created by liyu on 3/10/23.
//

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
