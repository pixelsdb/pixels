#include "stdlib.h"

#ifndef _Included_memory_mapped_file
#define _Included_memory_mapped_file

typedef struct
{
  char *addr;
  long size;
} MemoryMappedFile;

#define GET_INT(mmap_f, pos) ( *((int *)(mmap_f.addr + pos)) )
#define GET_BYTES(mmap_f, pos) ( (char *)(mmap_f.addr + pos) )

static inline int getInt(const MemoryMappedFile mmap_f, long pos)
{
  int *addr = (int *)(mmap_f.addr + pos);
  return *addr;
}

static inline int getLong(const MemoryMappedFile mmap_f, long pos)
{
  long *addr = (long *)(mmap_f.addr + pos);
  return *addr;
}

static inline void writeLong(const MemoryMappedFile mmap_f, long pos, long value) 
{
    long *addr = (long *) (mmap_f.addr + pos);
    *addr = value;
}

static inline void writeInt(const MemoryMappedFile mmap_f, long pos, int value) 
{
    int *addr = (int *) (mmap_f.addr + pos);
    *addr = value;
}

static inline const char *getBytes(const MemoryMappedFile mmap_f, long pos)
{
  return (char *)(mmap_f.addr + pos);
}


int valid(const MemoryMappedFile mmap_f)
{
  return mmap_f.addr != NULL;
}
#endif
