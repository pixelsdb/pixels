//
// Created by liyu on 3/10/23.
//

#include "physical/SchedulerFactory.h"

SchedulerFactory * SchedulerFactory::instance = nullptr;

SchedulerFactory * SchedulerFactory::Instance() {
    if(instance == nullptr) {
        instance = new SchedulerFactory();
    }
    return instance;
}

Scheduler *SchedulerFactory::getScheduler() {
    return scheduler;
}

SchedulerFactory::SchedulerFactory() {
    // TODO: here we read name from pixels.properties
    std::string name = ConfigFactory::Instance().getProperty("read.request.scheduler");
    std::transform(name.begin(), name.end(), name.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    if(name == "noop") {
        scheduler = NoopScheduler::Instance();
    } else if(name == "sortmerge") {
        scheduler =  SortMergeScheduler::Instance();
    } else {
        throw std::runtime_error("the read request scheduler is not support. ");
    }
}

SchedulerFactory::~SchedulerFactory() {
	delete instance;
	instance = nullptr;
}
