//
// Created by liyu on 3/8/23.
//

#include "physical/scheduler/NoopScheduler.h"
#include "exception/InvalidArgumentException.h"
#include "physical/io/PhysicalLocalReader.h"

Scheduler * NoopScheduler::instance = nullptr;

Scheduler * NoopScheduler::Instance() {
    if(instance == nullptr) {
        instance = new NoopScheduler();
    }
    return instance;
}

std::vector<std::shared_ptr<ByteBuffer>> NoopScheduler::executeBatch(std::shared_ptr<PhysicalReader> reader,
                                                                     RequestBatch batch, long queryId) {
	return executeBatch(reader, batch, {}, queryId);
}


std::vector<std::shared_ptr<ByteBuffer>> NoopScheduler::executeBatch(std::shared_ptr<PhysicalReader> reader, RequestBatch batch,
                                      std::vector<std::shared_ptr<ByteBuffer>> reuseBuffers, long queryId) {
	auto requests = batch.getRequests();
	std::vector<std::shared_ptr<ByteBuffer>> results;
	results.resize(batch.getSize());
	if(ConfigFactory::Instance().boolCheckProperty("localfs.enable.async.io") && reuseBuffers.size() > 0) {
		// async read
		auto localReader = std::static_pointer_cast<PhysicalLocalReader>(reader);
		for(int i = 0; i < batch.getSize(); i++) {
			Request request = requests[i];
			localReader->seek(request.start);
			results.at(i) = localReader->readAsync(request.length, reuseBuffers.at(i), request.bufferId);
		}
        localReader->readAsyncSubmit(batch.getSize());
	} else {
		// sync read
		for(int i = 0; i < batch.getSize(); i++) {
			Request request = requests[i];
			reader->seek(request.start);
			if(reuseBuffers.size() > 0) {
				results.at(i) = reader->readFully(request.length, reuseBuffers.at(i));
			} else {
				results.at(i) = reader->readFully(request.length);
			}

		}
	}
	return results;

}



NoopScheduler::~NoopScheduler() {
	delete instance;
	instance = nullptr;
}
