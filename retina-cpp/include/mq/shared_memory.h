#ifndef MEMORY_MAPPED_FILE_H
#define MEMORY_MAPPED_FILE_H

#include <fcntl.h>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "constants.h"
#include "type.h"

namespace stm {
enum MemType { MEM_CPU, MEM_GPU, MEM_NVM, MEM_NVMe };

class SharedMemory {
private:
  int _fd;
  struct stat _st;
  byte *_mapped;
  long _size;
  std::string _location;

  static inline long roundTo4096(long i) { return (i + 0xfffL) & ~0xfffL; }

  /**
   * TODO: Numa affinity.
   * Numa affinity to be implemented using functions
   * in numa.h and numaif.h, or using the topology and memory
   * manager in Proteus.
   */
  void mapAndSetOffset();

  void unmap() {
    std::cout << "munmap file '" << _location << "'" << std::endl;
    munmap(_mapped, _st.st_size);
  }

public:
  static const int Edian = BIG_ENDIAN;
  /**
   * Constructs a new memory mapped file.
   *
   * @param location the file name
   * @param size the file length
   */
  SharedMemory(const std::string &location, long size)
      : _location(location),
        _size(roundTo4096(
            size)) { // FIXME: test this location, as default was set by context
    std::cout << "MAP AND OFFSET " << _location << ", size = "<<
    roundTo4096(size) << std::endl;
    mapAndSetOffset();
  }

  SharedMemory()
      : _fd(-1), _mapped(nullptr), _location(""), _size(0) {
  } // FIXME: test this location, as default was set by context

  SharedMemory(const SharedMemory &) = delete;
  //        SharedMemory(const SharedMemory &copy) :
  //                _fd(copy._fd), _st(copy._st), _mapped(copy._mapped),
  //                _location(copy._location), _size(copy._size) { }

  ~SharedMemory() { unmap(); }

  /**
   * Synchronize the content with the mapped file.
   * @param pos the byte position in the shared memory.
   * @param length the length in bytes to synchronize.
   */
  void sync(long pos, long length) const {
    msync(_mapped + pos, length, MS_SYNC);
  }

  bool dropPageCache();

  void clear();

  /**
   * Reads a buffer of data.
   *
   * @param pos    the position in the util mapped file
   * @param buf   the input buffer
   * @param offset the offset in the buffer of the first byte to read data into
   * @param length the length of the data
   */
  void getBytes(long pos, byte *buf, int offset, int length) const;

  /**
   * Writes a buffer of data.
   *
   * @param pos    the position in the util mapped file
   * @param buf   the output buffer
   * @param offset the offset in the buffer of the first byte to write
   * @param length the length of the data
   */
  void setBytes(long pos, byte *buf, int offset, int length) const;

  byte getByte(long pos) const;

  short getShort(long pos) const;

  int getInt(long pos) const;

  long getLong(long pos) const;

  void setByte(long pos, byte val) const;

  void setShort(long pos, short val) const;

  void setInt(long pos, int val) const;

  void setLong(long pos, long val) const;

  bool compareAndSwapInt(long pos, int &expected, int value) const;

  bool compareAndSwapLong(long pos, long &expected, long value) const;

  long getAndAddLong(long pos, long delta) const;

  byte *getAddr(long pos) const { return _mapped + pos; }

  long size() const { return _size; }

  const std::string &location() const { return _location; }

  int getFD() const { return _fd; }
};
} // namespace stm

#endif // STORAGE_MANAGER_MEMORY_MAPPED_FILE_H
