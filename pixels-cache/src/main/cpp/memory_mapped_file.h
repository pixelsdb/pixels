#include "stdlib.h"

#ifndef _Included_memory_mapped_file
#define _Included_memory_mapped_file

typedef struct
{
  char *addr;
  long size;
} MemoryMappedFile;

int getInt(const MemoryMappedFile mmap_f, long pos)
{
  int *addr = (int *)(mmap_f.addr + pos);
  return *addr;
}

int getLong(const MemoryMappedFile mmap_f, long pos)
{
  long *addr = (long *)(mmap_f.addr + pos);
  return *addr;
}

void writeLong(const MemoryMappedFile mmap_f, long pos, long value) 
{
    long *addr = (long *) (mmap_f.addr + pos);
    *addr = value;
}

const char *getBytes(const MemoryMappedFile mmap_f, long pos)
{
  return (char *)(mmap_f.addr + pos);
}


int valid(const MemoryMappedFile mmap_f)
{
  return mmap_f.addr != NULL;
}
#endif
