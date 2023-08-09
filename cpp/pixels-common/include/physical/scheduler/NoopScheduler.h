//
// Created by liyu on 3/8/23.
//

#ifndef PIXELS_NOOPSCHEDULER_H
#define PIXELS_NOOPSCHEDULER_H

#include "physical/Scheduler.h"

class NoopScheduler : public Scheduler {
    // TODO: logger
public:
    static Scheduler * Instance();
	std::vector<std::shared_ptr<ByteBuffer>> executeBatch(std::shared_ptr<PhysicalReader> reader, RequestBatch batch, long queryId) override;
	std::vector<std::shared_ptr<ByteBuffer>> executeBatch(std::shared_ptr<PhysicalReader> reader, RequestBatch batch,
	                                                      std::vector<std::shared_ptr<ByteBuffer>> reuseBuffers, long queryId) override;
	~NoopScheduler();
private:
    static Scheduler * instance;
};
#endif //PIXELS_NOOPSCHEDULER_H
