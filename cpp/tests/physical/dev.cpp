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
 * @create 2023-02-27
 */
#include <gtest/gtest.h>
#include "physical/storage/LocalFS.h"
#include "physical/natives/ByteBuffer.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/io/PhysicalLocalReader.h"
#include "physical/StorageFactory.h"
#include "PixelsReaderImpl.h"
#include "PixelsReaderBuilder.h"
#include <iostream>
#include <future>
#include <thread>
#include "physical/scheduler/NoopScheduler.h"
#include "physical/SchedulerFactory.h"
#include "utils/Constants.h"
#include "exception/PixelsFileMagicInvalidException.h"
#include <vector>
#include "liburing.h"
#include "utils/String.h"
#include "liburing/io_uring.h"
#include "vector/LongColumnVector.h"



TEST(physical, StorageFunctionTest) {
    EXPECT_EQ(Storage::file, Storage::from("FiLe"));
    EXPECT_NE(Storage::s3, Storage::from("s4"));
    EXPECT_EQ(Storage::minio, Storage::fromPath("minio:///hello/world"));
    EXPECT_EQ(Storage::file, Storage::fromPath("file:///home/liyu"));
    EXPECT_TRUE(Storage::isValid("file"));
    EXPECT_FALSE(Storage::isValid("sssss"));

    LocalFS localfs;
    EXPECT_EQ(localfs.getScheme(), Storage::file);
    EXPECT_EQ("file:///home/liyu", localfs.ensureSchemePrefix("/home/liyu"));
    EXPECT_EQ("file:///home/liyu", localfs.ensureSchemePrefix("file:///home/liyu"));
    EXPECT_THROW(localfs.ensureSchemePrefix("sss:///home/liyu"), std::invalid_argument);
    EXPECT_THROW(localfs.ensureSchemePrefix(":///home/liyu"), std::invalid_argument);
    EXPECT_THROW(localfs.ensureSchemePrefix("s3:///home/liyu"), std::invalid_argument);
}

TEST(physical, ByteBufferTest) {
    auto bb1 = std::make_unique<ByteBuffer>(30);
    bb1->printPosition();
    printf("bb1 bytes Remaining: %i\n", bb1->bytesRemaining());
    bb1->putInt(134);
    bb1->printPosition();
    EXPECT_EQ((int)bb1->get(), 134);
    EXPECT_EQ((int)bb1->get(), 0);
    EXPECT_EQ((int)bb1->get(), 0);
    EXPECT_EQ((int)bb1->get(), 0);
}

TEST(physical, DirectRandomAccessFile) {
    DirectRandomAccessFile p("/home/liyu/files/hello.txt");
    std::shared_ptr<ByteBuffer> bb1 = p.readFully(3);
    std::shared_ptr<ByteBuffer> bb2 = p.readFully(1);
    std::shared_ptr<ByteBuffer> bb3 = p.readFully(2);
    std::shared_ptr<ByteBuffer> bb4 = p.readFully(4);
    std::shared_ptr<ByteBuffer> bb5 = p.readFully(5);
    p.close();
}

TEST(physical, PhysicalReader) {
    auto localfs = std::make_shared<LocalFS>();
    PhysicalLocalReader localReader(localfs, "/home/liyu/files/hello.txt");
    std::shared_ptr<ByteBuffer> bb1 = localReader.readFully(3);
    std::shared_ptr<ByteBuffer> bb2 = localReader.readFully(1);
    std::shared_ptr<ByteBuffer> bb3 = localReader.readFully(2);
    std::shared_ptr<ByteBuffer> bb4 = localReader.readFully(4);
    std::shared_ptr<ByteBuffer> bb5 = localReader.readFully(5);
    localReader.seek(11);
    char c = localReader.readChar();
    char d = localReader.readChar();
    localReader.close();
}

TEST(physical, RandomFile) {
    auto localfs = std::make_shared<LocalFS>();
    std::string path = "/home/liyu/files/file_64M";
    int fd = open(path.c_str(), O_RDONLY);
    long len = 64 * 1024 * 1024;
    auto * buffer = new uint8_t[len];
    if(pread(fd, buffer, len, 0) == -1) {
        throw std::runtime_error("the open file fail!");
    }
    ::close(fd);

    auto localReader = std::make_shared<PhysicalLocalReader>(localfs, path);

    std::shared_ptr<ByteBuffer> bb1 = localReader->readFully(3);
    std::shared_ptr<ByteBuffer> bb2 = localReader->readFully(1);
    std::shared_ptr<ByteBuffer> bb3 = localReader->readFully(2);
    std::shared_ptr<ByteBuffer> bb4 = localReader->readFully(4);
    std::shared_ptr<ByteBuffer> bb5 = localReader->readFully(5);
    localReader->seek(10);
    localReader->close();
}


TEST(physical, StorageFactory) {
    StorageFactory * sf = StorageFactory::getInstance();
    auto enabledSchemes = sf->getEnabledSchemes();
    EXPECT_EQ(Storage::file, enabledSchemes[0]);
    EXPECT_EQ(1, enabledSchemes.size());
    EXPECT_TRUE(sf->isEnabled(Storage::file));
    EXPECT_FALSE(sf->isEnabled(Storage::s3));
    sf->reloadAll();
    sf->reloadAll();
    std::cout<<"finish"<<std::endl;
}


TEST(physical, NoopScheduler) {
    Scheduler * noop = SchedulerFactory::Instance()->getScheduler();
    long queryId = 0;
    auto localfs = std::make_shared<LocalFS>();
    RequestBatch batch;
    batch.add(queryId, 5, 9);
    batch.add(queryId, 1, 4);
    batch.add(queryId, 2, 5);
    batch.add(queryId, 3, 7);
    auto localReader = std::make_shared<PhysicalLocalReader>(localfs, "/home/liyu/files/hello.txt");
    auto bbs = noop->executeBatch(localReader, batch, queryId);
    std::cout<<"fuck"<<std::endl;
}

TEST(utils, Constants) {
    std::cout<<Constants::AI_LOCK_PATH_PREFIX<<std::endl;
    std::cout<<Constants::INIT_DICT_SIZE<<std::endl;
    std::vector<bool> a;
    a.resize(10);
    std::cout<<a.at(0)<<std::endl;
}

TEST(physical, vector) {
    std::vector<int> a;
    a.reserve(10);
    a.emplace_back(1);
    std::cout<<a.size()<<std::endl;
    a.emplace_back(1);
    std::cout<<a.size()<<std::endl;

}

TEST(physical, uring) {
    std::vector<std::string> paths;
    paths.emplace_back("/home/yuly/demo/uringDemo/a");
    paths.emplace_back("/home/yuly/demo/uringDemo/b");
    paths.emplace_back("/home/yuly/demo/uringDemo/c");
    paths.emplace_back("/home/yuly/demo/uringDemo/d");
    std::vector<int> fds;
    for(auto path: paths) {
        int fd = open(path.c_str(), O_RDONLY);
		assert(fd >= 0);
        fds.emplace_back(fd);
    }
    int size = 6;
    std::vector<char *> bufs;
    for(int i = 0; i < paths.size(); i++) {
        char * buffer = new char[size];
        bufs.emplace_back(buffer);
    }

    struct io_uring ring{};
    if(io_uring_queue_init(4, &ring, 0) < 0) {
        throw std::runtime_error("initialize io_uring fails.");
    }


    for(int i = 0; i < paths.size(); i++) {
		struct io_uring_sqe * sqe = io_uring_get_sqe(&ring);
        io_uring_prep_read(sqe, fds[i], bufs[i], size, 0);
        io_uring_sqe_set_data(sqe, &bufs[i]);
		io_uring_submit(&ring);
    }

    for(int i = 0; i < paths.size(); i++) {
		struct io_uring_cqe *cqe;
		int ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret < 0) {
			perror("io_uring_wait_cqe");
		}
		if (cqe->res < 0) {
			fprintf(stderr, "Async readv failed.\n");
		}
		char ** buffer = (char **)io_uring_cqe_get_data(cqe);
		std::cout<<*buffer<<std::endl;
		io_uring_cqe_seen(&ring, cqe);
    }

    io_uring_queue_exit(&ring);
//	for(int i = 0; i < paths.size(); i++) {
//		std::cout<<bufs[i]<<std::endl;
//	}
}


TEST(physical, columnVector) {
    LongColumnVector a;
    a.close();
}


