#include "stdio.h"
#include "string.h"
#include "byteswap.h"
#include "io_pixelsdb_pixels_cache_CacheIndexSerializer.h"

#define INDEX_RADIX_OFFSET 16
#define KEY_LEN 12

unsigned long max_offset = 0l;

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

int valid(const MemoryMappedFile mmap_f)
{
  return mmap_f.addr != NULL;
}

MemoryMappedFile build_mmap_f(JNIEnv *env, jobject obj)
{
  jfieldID fid; /* store the field ID */
  jclass cls = (*env)->GetObjectClass(env, obj);
  /* Look for the instance field s in cls */
  fid = (*env)->GetFieldID(env, cls, "indexFile", "Lio/pixelsdb/pixels/cache/MemoryMappedFile;");
  if (fid == NULL)
  {
    return (MemoryMappedFile){NULL, -1}; /* failed to find the field */
  }
  jobject index_file = (*env)->GetObjectField(env, obj, fid);
  jclass mmap_file = (*env)->GetObjectClass(env, index_file);
  // parse file location
  fid = (*env)->GetFieldID(env, mmap_file, "addr", "J");
  jlong addr = (*env)->GetLongField(env, index_file, fid);
  fid = (*env)->GetFieldID(env, mmap_file, "size", "J");
  jlong size = (*env)->GetLongField(env, index_file, fid);

  return (MemoryMappedFile){(char *)addr, size};
}

// each key has at most 12 bytes, so sizeof(keyBuf)=12
void dfs(MemoryMappedFile indexFile, long currentNodeOffset, unsigned char *keyBuf, int ptr, FILE *out)
{
  if (currentNodeOffset > max_offset) max_offset = currentNodeOffset;
  // we reach a leaf, truly
  if (ptr == KEY_LEN) {
    fputs("0x", out);
    for(int i = 0; i < ptr; ++i) {
      fprintf(out, "%02x", keyBuf[i]);
      // fputc(keyBuf[i], out);
    }
    fputc(';', out);
    // parse the block id
    unsigned long blockId = bswap_64(*((unsigned long *) keyBuf));
    unsigned short rowGroupId = bswap_16(*((unsigned short *) (keyBuf + 8)));
    unsigned short columnId = bswap_16(*((unsigned short *) (keyBuf + 10)));
    fprintf(out, "%lu-%u-%u", blockId, rowGroupId, columnId);
    fputc('\n', out);
    // printf("ptr=%d, key=0x", ptr);
    // for(int i = 0; i < ptr; i++) {
    //   printf("%02x", keyBuf[i]);
    // }    
    // printf("\n");
    return;
  }

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

  const char *nodeData = getBytes(indexFile, currentNodeOffset + 4);

  // read the edge first
  int edgeEndOffset = currentNodeChildrenNum * 8 + currentNodeEdgeSize;
  for (int i = currentNodeChildrenNum * 8 + 1; i < edgeEndOffset; i++)
  {
    keyBuf[ptr++] = nodeData[i];
  }

  if (ptr == KEY_LEN) {
    fputs("0x", out);
    for(int i = 0; i < ptr; ++i) {
      // fputc(keyBuf[i], out);
      fprintf(out, "%02x", keyBuf[i]);

    }
    fputc(';', out);
    // parse the block id
    unsigned long blockId = bswap_64(*((unsigned long *) keyBuf));
    unsigned short rowGroupId = bswap_16(*((unsigned short *) (keyBuf + 8)));
    unsigned short columnId = bswap_16(*((unsigned short *) (keyBuf + 10)));
    fprintf(out, "%lu-%u-%u", blockId, rowGroupId, columnId);
    fputc('\n', out);
    // printf("ptr=%d, key=0x", ptr);
    // for(int i = 0; i < ptr; i++) {
    //   printf("%02x", keyBuf[i]);
    // }    
    // printf("\n");
    return;
  }

  if (currentNodeChildrenNum == 0) {
    printf("currentNodeChildrenNum is 0, but ptr=%d(not %d)\n", ptr, KEY_LEN);
    return;
  }

  // then children
  for (int i = 0; i < currentNodeChildrenNum; ++i)
  {
    // Note: the child is stored in big-endian!
    unsigned long child = (*((unsigned long *)nodeData));
    nodeData += 8;
    char leader = (char)((child >> 56) & 0xFF);
    long matchingChildOffset = (child & 0x00FFFFFFFFFFFFFF);
    // printf("child=0x%08lx, leader=0x%08x, nextChildrenAddress=%p\n", child, leader, (char *) matchingChildOffset);

    // add the leader to the keyBuf
    keyBuf[ptr++] = leader;
    dfs(indexFile, matchingChildOffset, keyBuf, ptr, out);
    --ptr;
  }
}

JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_cache_CacheIndexSerializer_traverse(JNIEnv *env, jobject obj)
{
  MemoryMappedFile indexFile = build_mmap_f(env, obj);
  if (!valid(indexFile))
  {
    return;
  }
  jlong addr = (jlong)indexFile.addr;
  jlong size = indexFile.size;
  // parse the index header
  printf("native ------------------------------ hello cache serializer!\n");
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
  unsigned int currentNodeHeader = getInt(indexFile, currentNodeOffset);
  printf("currentNodeHeader=0x%08x, currentNodeHeader=%u\n", currentNodeHeader, currentNodeHeader);
  const unsigned int currentNodeChildrenNum = currentNodeHeader & 0x000001FF;
  const unsigned int currentNodeEdgeSize = (currentNodeHeader & 0x7FFFFE00) >> 9;
  printf("currentNodeChildrenNum=%u, currentNodeEdgeSize=%u\n", currentNodeChildrenNum, currentNodeEdgeSize);
  const char *nodeData = getBytes(indexFile, currentNodeOffset + 4);

  FILE* out = fopen("./tmp.txt", "w");
  char keyBuf[12] = {0};
  dfs(indexFile, currentNodeOffset, keyBuf, 0, out);
  fclose(out);

  printf("max offset=%lu\n", max_offset);

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
