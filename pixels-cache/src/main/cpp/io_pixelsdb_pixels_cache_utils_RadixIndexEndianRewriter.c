#include "stdio.h"
#include "string.h"
#include "byteswap.h"
#include "utils.h"
#include "memory_mapped_file.h"
#include "io_pixelsdb_pixels_cache_utils_RadixIndexEndianRewriter.h"

#define INDEX_RADIX_OFFSET 16
#define KEY_LEN 12

// each key has at most 12 bytes, so sizeof(keyBuf)=12
void dfs(MemoryMappedFile indexFile, long currentNodeOffset, unsigned char *keyBuf, int ptr)
{
  // we reach a deadend
  if ((char *) currentNodeOffset == NULL)
  { 
    printf("currentNodeOffset is NULL, but ptr=%d(not %d)\n", ptr, KEY_LEN);
    return;
  }

  // we have something
  unsigned long currentNodeHeader = getInt(indexFile, currentNodeOffset);
  unsigned int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
  unsigned int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >> 9;
  int childOffset = currentNodeOffset + 4;
  const char *nodeData = getBytes(indexFile, childOffset);

  // read the edge first
  int edgeEndOffset = currentNodeChildrenNum * 8 + currentNodeEdgeSize;
  for (int i = currentNodeChildrenNum * 8 + 1; i < edgeEndOffset; i++)
  {
    keyBuf[ptr++] = nodeData[i];
  }

  // we reach a leaf, truly
  if (ptr == KEY_LEN) {
    // rewrite the cacheIdx
    long pos = currentNodeOffset + 4 + (currentNodeChildrenNum * 8) + currentNodeEdgeSize;
    const char* cacheIdx = getBytes(indexFile, pos);
    unsigned long offset = bswap_64(*((unsigned long *) cacheIdx));
    unsigned int length = bswap_32(*((unsigned int *) (cacheIdx + 8)));
    writeLong(indexFile, pos, offset);
    writeInt(indexFile, pos + 8, length);
    return;
  }

  // if (ptr == KEY_LEN) {
  //   return;
  // }

  if (currentNodeChildrenNum == 0) {
    printf("currentNodeChildrenNum is 0, but ptr=%d(not %d)\n", ptr, KEY_LEN);
    return;
  }

  // then children
  for (int i = 0; i < currentNodeChildrenNum; ++i)
  {
    // Note: the child is stored in big-endian!
    unsigned long child = bswap_64(*((unsigned long *)nodeData));
    char leader = (char)((child >> 56) & 0xFF);
    long matchingChildOffset = (child & 0x00FFFFFFFFFFFFFF);
    writeLong(indexFile, childOffset, child); // rewrite with little endian value
    nodeData += 8;
    childOffset += 8;
    // printf("child=0x%08lx, leader=0x%08x, nextChildrenAddress=%p\n", child, leader, (char *) matchingChildOffset);

    // add the leader to the keyBuf
    keyBuf[ptr++] = leader;
    dfs(indexFile, matchingChildOffset, keyBuf, ptr);
    --ptr;
  }
}

JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_cache_utils_RadixIndexEndianRewriter_rewrite(JNIEnv *env, jobject obj)
{
  MemoryMappedFile indexFile = build_mmap_f(env, obj, "indexFile");

  if (!valid(indexFile))
  {
    return;
  }
  jlong addr = (jlong)indexFile.addr;
  jlong size = indexFile.size;
  // parse the index header
  printf("native ------------------------------ hello cache rewriter!\n");
  printf("addr=%ld, addr=%p, size=%ld\n", addr, (char *)addr, size);

  // parse the header {magic(6)+rw_flag(1)+reader_count(3)+version(4)}
  const char *header = getBytes(indexFile, 0);
  char magic[7] = {0};
  int rw_flag;
  unsigned int reader_count = 0;
  int version;
  memcpy(magic, header, 6);
  rw_flag = (int)header[6];
  memcpy(((char *)&reader_count) + 1, header + 7, 3);
  version = *((int *)(header + 10));
  printf("HEADER: magic=%s, rw_flag=%d, reader_count=%x, version=%d\n", magic, rw_flag, reader_count, version);

  long currentNodeOffset = INDEX_RADIX_OFFSET;

  char keyBuf[12] = {0};
  dfs(indexFile, currentNodeOffset, keyBuf, 0);

  // while (1)
  // {
  //   // we do a dfs
  //   for (int i = 0; i < currentNodeChildrenNum; ++i)
  //   {
  //     // Note: the child is stored in big-endian!
  //     unsigned long child = bswap_64(*((unsigned long *)nodeData));
  //     nodeData += 8;
  //     int leader = (int)((child >> 56) & 0xFF);
  //     const char *matchingChildOffset = (char *)(child & 0x00FFFFFFFFFFFFFF);
  //     printf("child=0x%08lx, leader=0x%08x, nextChildrenAddress=%p\n", child, leader, matchingChildOffset);

  //     if (matchingChildOffset == NULL)
  //     {
  //       break;
  //     }
  //     currentNodeOffset = matchingChildOffset;
  //     currentNodeHeader = getInt(indexFile, currentNodeOffset);
  //   }
  // }

  return;
}
