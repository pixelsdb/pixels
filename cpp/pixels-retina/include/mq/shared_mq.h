//
// Created by bian on 2020-10-29.
//

#ifndef STORAGE_MANAGER_SHARED_MQ_H
#define STORAGE_MANAGER_SHARED_MQ_H

#include "error_code.h"
#include "shared_memory.h"
#include <string>

#define SHARED_MQ_STRUCT_READ_LIMIT 0L
#define SHARED_MQ_STRUCT_WRITE_LIMIT 8L
#define SHARED_MQ_STRUCT_INIT_FLAG 16L
// although only use 17 bytes are used for metadata, we use 24 bytes for word
// alignment.
#define SHARED_MQ_STRUCT_DATA 24L
#define SHARED_MQ_LIMIT_FLAG 0x4000000000000000L
#define SHARED_MQ_LIMIT_MASK 0x3FFFFFFFFFFFFFFFL
#define SHARED_MQ_LENGTH_MESSAGE_HEADER 2L
#define SHARED_MQ_LENGTH_MESSAGE_STATUS 1L
#define SHARED_MQ_DEFAULT_TIMEOUT_MS 1000
#define SHARED_MQ_INITIALIZED 0x66

#define SHARED_MQ_ENTRY_SIZE(message_size)                                     \
  (SHARED_MQ_LENGTH_MESSAGE_HEADER + message_size)
#define SHARED_MQ_REAL_LIMIT(limit) (SHARED_MQ_LIMIT_MASK & limit)
#define SHARED_MQ_FLIP_LIMIT(limit)                                            \
  (((limit ^ SHARED_MQ_LIMIT_FLAG) & SHARED_MQ_LIMIT_FLAG) |                   \
   SHARED_MQ_STRUCT_DATA)
#define SHARED_MQ_IS_FLIPPED(read_limit, write_limit)                          \
  ((read_limit & SHARED_MQ_LIMIT_FLAG) ^ (write_limit & SHARED_MQ_LIMIT_FLAG))

namespace stm {
enum TransactionStatus { CLEAR = 0, RUNNING = 1, ROLLBACK = 2, COMMIT = 3 };

long getSystemTimeMs();

long getSystemTimeNs();

class Message {
public:
  virtual ~Message() {}

  /**
   * Return the footprint of this message in the shared memory.
   * @return the footprint in bytes.
   */
  virtual int size() const = 0;

  /**
   * Read the content of this message from the given pos in sharedMemory.
   * @param sharedMemory the shared memory to read this message from.
   * @param pos the byte position in the shared memory.
   */
  virtual void read(const SharedMemory *sharedMemory, long pos) = 0;

  /**
   * Write the content of this message into the given pos in sharedMemory.
   * @param sharedMemory the shared memory to write this message into.
   * @param pos the byte position in the shared memory.
   */
  virtual void write(const SharedMemory *sharedMemory, long pos) const = 0;

  /**
   * This method is only used for debugging.
   * @param sharedMemory the shared memory to read and print this message.
   * @param pos the byte position in the shared memory.
   * @return the content of the message.
   */
  virtual std::string print(const SharedMemory *sharedMemory,
                            long pos) const = 0;
};

/**
 * The message queue in shared memory is safe for concurrent access.
 * However, this class is not thread safe, every thread should have it own
 * instance of SharedMQ to access the shared message queue.
 */
class SharedMQ {
private:
  SharedMemory *_sharedMemory;
  bool _internalShm;
  int _messageSize;
  int _timeout;
  bool _readLimitPushed;
  bool _writeLimitPushed;
  long _readLimit;
  long _writeLimit;
  long _readStartTime;
  long _writeStartTime;
  long _EOQ; // end of queue.

  int pushWriteLimit();

  int pushReadLimit();

  bool isEmpty(long readLimit, long writeLimit);

  bool isFull(long writeLimit, long readLimit);

  SharedMQ() = delete;

public:
  SharedMQ(const std::string &location, long totalSize, int messageSize,
           bool forceInit = false)
      : _messageSize(messageSize), _timeout(SHARED_MQ_DEFAULT_TIMEOUT_MS),
        _readLimitPushed(false), _writeLimitPushed(false),
        _readLimit(SHARED_MQ_STRUCT_DATA), _writeLimit(SHARED_MQ_STRUCT_DATA),
        _readStartTime(0), _writeStartTime(0) {
    _sharedMemory = new SharedMemory(location, totalSize);
    _internalShm = true;
    _EOQ = _sharedMemory->size();
    _EOQ -= (_EOQ - SHARED_MQ_STRUCT_DATA) % SHARED_MQ_ENTRY_SIZE(_messageSize);
    init(forceInit);
  }

  SharedMQ(SharedMemory *const sharedMemory, int messageSize,
           bool forceInit = false)
      : _messageSize(messageSize), _timeout(SHARED_MQ_DEFAULT_TIMEOUT_MS),
        _readLimitPushed(false), _writeLimitPushed(false),
        _readLimit(SHARED_MQ_STRUCT_DATA), _writeLimit(SHARED_MQ_STRUCT_DATA),
        _readStartTime(0), _writeStartTime(0) {
    _sharedMemory = sharedMemory;
    _internalShm = false;
    _EOQ = _sharedMemory->size();
    _EOQ -= (_EOQ - SHARED_MQ_STRUCT_DATA) % SHARED_MQ_ENTRY_SIZE(_messageSize);
    init(forceInit);
  }

  SharedMQ(const std::string &location, long totalSize, int messageSize,
           int timeout, bool forceInit = false)
      : _messageSize(messageSize), _timeout(timeout), _readLimitPushed(false),
        _writeLimitPushed(false), _readLimit(SHARED_MQ_STRUCT_DATA),
        _writeLimit(SHARED_MQ_STRUCT_DATA), _readStartTime(0),
        _writeStartTime(0) {
    _sharedMemory = new SharedMemory(location, totalSize);
    _internalShm = true;
    _EOQ = _sharedMemory->size();
    _EOQ -= (_EOQ - SHARED_MQ_STRUCT_DATA) % SHARED_MQ_ENTRY_SIZE(_messageSize);
    init(forceInit);
  }

  ~SharedMQ() {
    if (_internalShm) {
      delete _sharedMemory;
    }
  }

  /**
   * This method initializes the message queue in shared memory.
   * It is reentrant.
   */
  void init(bool force = false);

  /**
   * This method should be called before readMessage.
   * @return the error code or SUCCESS.
   */
  int nextForRead();

  /**
   * Read the message from message queue.
   * @param message the message to read to.
   */
  void readMessage(Message &message);

  /**
   * This method should be called before writeMassage.
   * @return the error code or SUCCESS.
   */
  int nextForWrite();

  /**
   * Write the given massage into message queue.
   * @param message the given message.
   */
  void writeMessage(const Message &message);

  /**
   * This method is only used for debugging.
   * @param message the message to print.
   */
  void print(const Message &message);
};

/*
// We remove RequestQueue because:
// 1. CatalogServer and CatalogService should not initialize request queue in
the same way,
// 2. Request queue must be deconstructed after CatalogServer and CatalogService
(this is not ensured if we use RequestQueue singleton),
// 3. It does not help preventing CatalogServer and CatalogService from
reconstructing the response queues.
// To avoid reconstruction of the request queue in the same process, we have add
some check logics in CatalogServer and CatalogService.

class RequestQueue {
  RequestQueue()
      : _mainRequestMQ(CATALOG_MAIN_REQ_MQ, CATALOG_MQ_FILE_SIZE,
                       CATALOG_MESSAGE_SIZE, true) {}

  SharedMQ _mainRequestMQ;

public:
  static SharedMQ &getInstance() {
    static RequestQueue instance;
    return instance._mainRequestMQ;
  }

  RequestQueue(const RequestQueue &) = delete;
  void operator=(const RequestQueue &) = delete;

  RequestQueue(RequestQueue &&) = delete;
  RequestQueue &operator=(RequestQueue &&) = delete;
};
*/
} // namespace stm

#endif // STORAGE_MANAGER_SHARED_MQ_H
