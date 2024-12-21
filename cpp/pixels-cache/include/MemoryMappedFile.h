/*
 * Copyright 2021 PixelsDB.
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
 * @author hank
 */
#include <stdlib.h>

#ifndef _Included_memory_mapped_file
#define _Included_memory_mapped_file

typedef struct
{
  char *addr;
  long size;
} MemoryMappedFile;

#define GET_INT(mmap_f, pos) ( *((int *)(mmap_f.addr + pos)) )
#define GET_LONG(mmap_f, pos) ( *((long *)(mmap_f.addr + pos)) )
#define GET_BYTES(mmap_f, pos) ( (char *)(mmap_f.addr + pos) )

static inline int getInt(const MemoryMappedFile mmap_f, long pos)
{
  int *addr = (int *)(mmap_f.addr + pos);
  return *addr;
}

static inline long getLong(const MemoryMappedFile mmap_f, long pos)
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
