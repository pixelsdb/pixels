//
// Created by bian on 2020-10-29.
//

#include "mq/shared_mq.h"
#include <chrono>
#include <iostream>

namespace stm {
long getSystemTimeMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

long getSystemTimeNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

bool SharedMQ::isEmpty(long readLimit, long writeLimit) {
  /**
   * We do not have any limit that equals to _EOQ, so that
   * we do not deal with this case here.
   */
  if (SHARED_MQ_IS_FLIPPED(readLimit, writeLimit)) {
    return false;
  }
  return SHARED_MQ_REAL_LIMIT(readLimit) >= SHARED_MQ_REAL_LIMIT(writeLimit);
}

bool SharedMQ::isFull(long writeLimit, long readLimit) {
  /**
   * We do not have any limit that equals to _EOQ, so that
   * we do not deal with this case here.
   */
  if (SHARED_MQ_IS_FLIPPED(readLimit, writeLimit)) {
    return SHARED_MQ_REAL_LIMIT(writeLimit) >= SHARED_MQ_REAL_LIMIT(readLimit);
  }
  return false;
}

int SharedMQ::pushWriteLimit() {
  _writeLimit = _sharedMemory->getLong(SHARED_MQ_STRUCT_WRITE_LIMIT);
  while (true) {
    // check if the mq is full.
    if (isFull(_writeLimit,
               _sharedMemory->getLong(SHARED_MQ_STRUCT_READ_LIMIT))) {
      return ERROR_MQ_IS_FULL;
    }
    // push write limit.
    if (SHARED_MQ_REAL_LIMIT(_writeLimit) +
            SHARED_MQ_ENTRY_SIZE(_messageSize) >=
        _EOQ) {
      /**
       * We have reached the end of the shared memory, try to flip to the
       * beginning. If the flip is not successful, we get the latest write limit
       * and retry the allocation.
       */
      if (_sharedMemory->compareAndSwapLong(
              SHARED_MQ_STRUCT_WRITE_LIMIT, _writeLimit,
              SHARED_MQ_FLIP_LIMIT(_writeLimit))) {
        break;
      }
    } else if (_sharedMemory->compareAndSwapLong(
                   SHARED_MQ_STRUCT_WRITE_LIMIT, _writeLimit,
                   _writeLimit + SHARED_MQ_ENTRY_SIZE(_messageSize))) {
      break;
    }
  }
  _writeLimit = SHARED_MQ_REAL_LIMIT(_writeLimit);
  _writeLimitPushed = true;
  return STM_SUCCESS;
}

int SharedMQ::pushReadLimit() {
  _readLimit = _sharedMemory->getLong(SHARED_MQ_STRUCT_READ_LIMIT);
  while (true) {
    // check if the mq is empty.
    if (isEmpty(_readLimit,
                _sharedMemory->getLong(SHARED_MQ_STRUCT_WRITE_LIMIT))) {
      return ERROR_MQ_IS_EMPTY;
    }
    // push the read limit.
    if (SHARED_MQ_REAL_LIMIT(_readLimit) + SHARED_MQ_ENTRY_SIZE(_messageSize) >=
        _EOQ) {
      /**
       * We have reached the end of the queue, try to flip to the beginning.
       * If the flip is failed, it means that another reader has already
       * finished the flip. Thus we have get the latest read limit and retry.
       * If the flip is successful, we continue pushing the read limit.
       */
      if (_sharedMemory->compareAndSwapLong(SHARED_MQ_STRUCT_READ_LIMIT,
                                            _readLimit,
                                            SHARED_MQ_FLIP_LIMIT(_readLimit))) {
        break;
      }
    } else if (_sharedMemory->compareAndSwapLong(
                   SHARED_MQ_STRUCT_READ_LIMIT, _readLimit,
                   _readLimit + SHARED_MQ_ENTRY_SIZE(_messageSize))) {
      break;
    }
  }
  _readLimit = SHARED_MQ_REAL_LIMIT(_readLimit);
  _readLimitPushed = true;
  return STM_SUCCESS;
}

int SharedMQ::nextForRead() {
  if (_readLimitPushed == false) {
    int ret = pushReadLimit();
    if (ret != STM_SUCCESS) {
      return ret;
    }
  }
  byte writerStatus = _sharedMemory->getByte(_readLimit);
  if (writerStatus == TransactionStatus::COMMIT) {
    _readStartTime = 0;
    _sharedMemory->setByte(_readLimit, TransactionStatus::CLEAR);
    _sharedMemory->setByte(_readLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS,
                           TransactionStatus::RUNNING);
    return STM_SUCCESS;
  }
  if (writerStatus == TransactionStatus::ROLLBACK) {
    _readStartTime = 0;
    _readLimitPushed = false;
    return ERROR_MQ_WRITER_IS_ROLLBACK;
  }
  // writerStatus == TransactionStatus::RUNNING
  if (_readStartTime == 0) {
    _readStartTime = getSystemTimeMs();
    return ERROR_MQ_WRITER_IS_RUNNING;
  } else if (getSystemTimeMs() - _readStartTime >= _timeout) {
    _sharedMemory->setByte(_readLimit, (byte)TransactionStatus::ROLLBACK);
    _readLimitPushed = false;
    _readStartTime = 0;
    return ERROR_MQ_WRITER_IS_ROLLBACK;
  }
  return ERROR_MQ_WRITER_IS_RUNNING;
}

void SharedMQ::readMessage(Message &message) {
  message.read(_sharedMemory, _readLimit + SHARED_MQ_LENGTH_MESSAGE_HEADER);
  _sharedMemory->setByte(_readLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS,
                         TransactionStatus::COMMIT);
  _readLimitPushed = false;
}

int SharedMQ::nextForWrite() {
  if (_writeLimitPushed == false) {
    int ret = pushWriteLimit();
    if (ret != STM_SUCCESS) {
      return ret;
    }
  }
  // check the status of the allocated entry.
  byte readerStatus =
      _sharedMemory->getByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS);
  if (readerStatus == TransactionStatus::COMMIT ||
      readerStatus == TransactionStatus::ROLLBACK ||
      readerStatus == TransactionStatus::CLEAR) {
    _writeStartTime = 0;
    _sharedMemory->setByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS,
                           TransactionStatus::RUNNING);
    _sharedMemory->setByte(_writeLimit, TransactionStatus::RUNNING);
    return STM_SUCCESS;
  }
  // readerStatus == TransactionStatus::RUNNING
  if (_writeStartTime == 0) {
    _writeStartTime = getSystemTimeMs();
    return ERROR_MQ_READER_IS_RUNNING;
  } else if (getSystemTimeMs() - _writeStartTime >= _timeout) {
    _sharedMemory->setByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS,
                           TransactionStatus::ROLLBACK);
    _writeStartTime = 0;
    _sharedMemory->setByte(_writeLimit, TransactionStatus::RUNNING);
    return STM_SUCCESS;
  }
  return ERROR_MQ_WRITER_IS_RUNNING;
}

void SharedMQ::writeMessage(const Message &message) {
  message.write(_sharedMemory, _writeLimit + SHARED_MQ_LENGTH_MESSAGE_HEADER);
  _sharedMemory->setByte(_writeLimit, TransactionStatus::COMMIT);
  _writeLimitPushed = false;
}

void SharedMQ::init(bool force) {
  if (force == true || _sharedMemory->getByte(SHARED_MQ_STRUCT_INIT_FLAG) !=
                           SHARED_MQ_INITIALIZED) {
    _sharedMemory->clear();
    _sharedMemory->setLong(SHARED_MQ_STRUCT_READ_LIMIT, SHARED_MQ_STRUCT_DATA);
    _sharedMemory->setLong(SHARED_MQ_STRUCT_WRITE_LIMIT, SHARED_MQ_STRUCT_DATA);
    _sharedMemory->setByte(SHARED_MQ_STRUCT_INIT_FLAG, SHARED_MQ_INITIALIZED);
  }
}

void SharedMQ::print(const Message &message) {
  std::cout << "------ Content in the Message Queue ------" << std::endl
            << "read limit: ("
            << ((_sharedMemory->getLong(SHARED_MQ_STRUCT_READ_LIMIT) &
                 SHARED_MQ_LIMIT_FLAG) > 0
                    ? "1"
                    : "0")
            << ")"
            << SHARED_MQ_REAL_LIMIT(
                   _sharedMemory->getLong(SHARED_MQ_STRUCT_READ_LIMIT))
            << std::endl
            << "write limit: ("
            << ((_sharedMemory->getLong(SHARED_MQ_STRUCT_WRITE_LIMIT) &
                 SHARED_MQ_LIMIT_FLAG) > 0
                    ? "1"
                    : "0")
            << ")"
            << SHARED_MQ_REAL_LIMIT(
                   _sharedMemory->getLong(SHARED_MQ_STRUCT_WRITE_LIMIT))
            << std::endl
            << "init flag: "
            << _sharedMemory->getLong(SHARED_MQ_STRUCT_INIT_FLAG) << std::endl;
  for (long i = SHARED_MQ_STRUCT_DATA, j = 0; i < _EOQ;
       i += SHARED_MQ_ENTRY_SIZE(_messageSize), ++j) {
    std::cout
        << "message " << j << ": read status ("
        << (int)_sharedMemory->getByte(i + SHARED_MQ_LENGTH_MESSAGE_STATUS)
        << "), write status (" << (int)_sharedMemory->getByte(i) << "), value ("
        << message.print(_sharedMemory, i + SHARED_MQ_LENGTH_MESSAGE_HEADER)
        << ")" << std::endl;
  }
}
} // namespace stm
