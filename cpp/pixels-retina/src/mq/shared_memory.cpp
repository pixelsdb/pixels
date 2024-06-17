#include "mq/shared_memory.h"
#include <cstring>
#include <iostream>
#include <numa.h>
#include <numaif.h>

using namespace stm;

void SharedMemory::mapAndSetOffset() {
  // create this file before opening it.
  if ((_fd = open(_location.c_str(), O_RDWR)) < 0) {
    std::cerr << "open error " << _fd << std::endl;
    exit(EXIT_FAILURE);
  }
  if ((fstat(_fd, &_st)) == -1) {
    std::cerr << "fstat error" << std::endl;
    exit(EXIT_FAILURE);
  }
  if (_st.st_size < _size) {
    std::cerr << "backed file is too small, "
              << "file size: " << _st.st_size << ", expected size: " << _size
              << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << "mmap file '" << _location << "', mapped size: " << _size
            << std::endl;

  _mapped =
      (byte *)mmap(nullptr, _size, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
  if (_mapped == (void *)-1) {
    std::cerr << "mmap error" << std::endl;
    exit(EXIT_FAILURE);
  }

  close(_fd); // mappped memory is still available after file closed.
}

bool SharedMemory::dropPageCache() {
  int fd = open(_location.c_str(), O_RDWR);
  if (fd < 0) {
    return false;
  }
  unmap();
  if (posix_fadvise(fd, 0, _size, POSIX_FADV_DONTNEED) != 0) {
    return false;
  }
  if (close(fd) != 0) {
    return false;
  }
  mapAndSetOffset();
  return true;
}

void SharedMemory::clear() { memset(_mapped, 0, _size); }

void SharedMemory::getBytes(long pos, byte *buf, int offset, int length) const {
  memcpy(buf + offset, _mapped + pos, length);
}

void SharedMemory::setBytes(long pos, byte *buf, int offset, int length) const {
  memcpy(_mapped + pos, buf + offset, length);
}

byte SharedMemory::getByte(long pos) const {
  return __atomic_load_n((byte *)(_mapped + pos), __ATOMIC_ACQUIRE);
}

short SharedMemory::getShort(long pos) const {
  return __atomic_load_n((short *)(_mapped + pos), __ATOMIC_ACQUIRE);
}

int SharedMemory::getInt(long pos) const {
  return __atomic_load_n((int *)(_mapped + pos), __ATOMIC_ACQUIRE);
}

long SharedMemory::getLong(long pos) const {
  return __atomic_load_n((long *)(_mapped + pos), __ATOMIC_ACQUIRE);
}

void SharedMemory::setByte(long pos, byte val) const {
  __atomic_store_n((byte *)(_mapped + pos), val, __ATOMIC_RELEASE);
}

void SharedMemory::setShort(long pos, short val) const {
  __atomic_store_n((short *)(_mapped + pos), val, __ATOMIC_RELEASE);
}

void SharedMemory::setInt(long pos, int val) const {
  __atomic_store_n((int *)(_mapped + pos), val, __ATOMIC_RELEASE);
}

void SharedMemory::setLong(long pos, long val) const {
  __atomic_store_n((long *)(_mapped + pos), val, __ATOMIC_RELEASE);
}

bool SharedMemory::compareAndSwapInt(long pos, int &expected, int value) const {
  return __atomic_compare_exchange_n((int *)(_mapped + pos), &expected, value,
                                     false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

bool SharedMemory::compareAndSwapLong(long pos, long &expected,
                                      long value) const {
  return __atomic_compare_exchange_n((long *)(_mapped + pos), &expected, value,
                                     false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

long SharedMemory::getAndAddLong(long pos, long delta) const {
  return __atomic_fetch_add((long *)(_mapped + pos), delta, __ATOMIC_SEQ_CST);
}
