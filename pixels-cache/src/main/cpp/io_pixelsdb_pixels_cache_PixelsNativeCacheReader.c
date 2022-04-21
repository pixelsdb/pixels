#include "stdio.h"
#include "string.h"
#include "byteswap.h"
#include "io_pixelsdb_pixels_cache_PixelsNativeCacheReader.h"

#define INDEX_RADIX_OFFSET 16
#define KEY_LEN 12       // long + short + short
#define CACHE_IDX_LEN 12 // long + int

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

const char *getBytes(const MemoryMappedFile mmap_f, long pos)
{
  return (char *)(mmap_f.addr + pos);
}

void buildKeyBuf(char *keyBuf, unsigned long blockId, unsigned short rowGroupId, unsigned short columnId)
{
  // underlying bytes should be big-endian
  unsigned long blockId_ = bswap_64(blockId);
  unsigned short rowGroupId_ = bswap_16(rowGroupId);
  unsigned short columnId_ = bswap_16(columnId);
  memcpy(keyBuf, &blockId_, 8);
  memcpy(keyBuf + 8, &rowGroupId_, 2);
  memcpy(keyBuf + 10, &columnId_, 2);
}

JNIEXPORT jlongArray JNICALL Java_io_pixelsdb_pixels_cache_PixelsNativeCacheReader_search(JNIEnv *env, jobject obj, jlong addr, jlong size, jlong blockId, jshort rowGroupId, jshort columnId)
{
  MemoryMappedFile indexFile = (MemoryMappedFile){(char *)addr, size};
  char keyBuf[KEY_LEN] = {0};
  buildKeyBuf(keyBuf, blockId, rowGroupId, columnId);
  int bytesMatched = 0;
  int bytesMatchedInNodeFound = 0;

  // init the return value
  long ret_[3] = {0, -1, -1};
  jlongArray ret = (*env)->NewLongArray(env, 3); // success, offset, length
  (*env)->SetLongArrayRegion(env, ret, 0, 3, ret_);

  // (*env)->byte
  // (*env)->SetByteArrayRegion(env, cacheIdxBytes, 0, CACHE_IDX_LEN, )

  long currentNodeOffset = INDEX_RADIX_OFFSET;
  unsigned int currentNodeHeader = getInt(indexFile, currentNodeOffset);
  // printf("currentNodeHeader=0x%08x, currentNodeHeader=%u\n", currentNodeHeader, currentNodeHeader);
  unsigned int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
  unsigned int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >> 9;
  if (currentNodeChildrenNum == 0 && currentNodeEdgeSize == 0)
  {
    return ret;
  }
  // printf("currentNodeChildrenNum=%u, currentNodeEdgeSize=%u\n", currentNodeChildrenNum, currentNodeEdgeSize);
  const char *nodeData = getBytes(indexFile, currentNodeOffset + 4);
  int cont = 1;
  while (bytesMatched < KEY_LEN && cont)
  {
    long matchingChildOffset = 0;
    for (int i = 0; i < currentNodeChildrenNum; ++i)
    {
      // long has 8 bytes, which is a child's bytes
      unsigned long child = bswap_64(*((unsigned long *)nodeData));
      nodeData += 8;
      // first byte is matching byte
      char leader = (char)((child >> 56) & 0xFF);
      matchingChildOffset = (child & 0x00FFFFFFFFFFFFFF);
      if (leader == keyBuf[bytesMatched])
      {
        // match a byte, then we can go to next level(byte)
        // last 7 bytes is the offset(pointer to the children position)
        matchingChildOffset = child & 0x00FFFFFFFFFFFFFFL;
        break;
      }
    }
    if (matchingChildOffset == 0) // cache miss
    {
      break;
    }

    currentNodeOffset = matchingChildOffset;
    bytesMatchedInNodeFound = 0;

    currentNodeHeader = getInt(indexFile, currentNodeOffset);
    currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
    currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >> 9;
    nodeData = getBytes(indexFile, currentNodeOffset + 4);
    int edgeEndOffset = currentNodeChildrenNum * 8 + currentNodeEdgeSize;
    ++bytesMatched;
    ++bytesMatchedInNodeFound;


    // now we are visiting the edge! rather than children data
    // it seems between children and edge, there is a one byte gap?
    // or the first byte of the edge does not matter here anyway
    for (int i = currentNodeChildrenNum * 8 + 1; i < edgeEndOffset && bytesMatched < KEY_LEN; ++i)
    {
      // the edge is shared across this node, so the edge should be fully matched
      if (nodeData[i] != keyBuf[bytesMatched])
      {
        cont = 0;
        break;
      }
      ++bytesMatched;
      ++bytesMatchedInNodeFound;
    }
  }

  // if matches, node found.
  // TODO: 终于找到原因为什么中间有一个 byte 的 gap 了. 因为 currentNodeEdgeSize 还包含了和某个 child match 的 byte
  if (bytesMatched == KEY_LEN && bytesMatchedInNodeFound == currentNodeEdgeSize)
  {
      // if the current node is leaf node.
      if (((currentNodeHeader >> 31) & 1) > 0) // TODO: why do we need & 1?
      {
          long ret_[] = {1, -1, -1};
          const char* cacheIdx = getBytes(indexFile, currentNodeOffset + 4 + (currentNodeChildrenNum * 8) + currentNodeEdgeSize);
          unsigned long offset = bswap_64(*((unsigned long *) cacheIdx));
          unsigned int length = bswap_32(*((unsigned int *) (cacheIdx + 8)));
          ret_[1] = offset;
          ret_[2] = length;
          (*env)->SetLongArrayRegion(env, ret, 0, 3, ret_);
      }
  }

  return ret;
}
