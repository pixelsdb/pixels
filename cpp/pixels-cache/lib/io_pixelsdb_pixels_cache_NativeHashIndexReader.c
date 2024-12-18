#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <byteswap.h>
#include "../include/MemoryMappedFile.h"
#include "../include/io_pixelsdb_pixels_cache_NativeHashIndexReader.h"

#define INDEX_HASH_HEAD_OFFSET 24 // 16 + 8
#define KEY_LEN 12       // long + short + short
#define CACHE_IDX_LEN 12 // long + int
#define KV_SIZE 24

int hashcode(const char* bytes, int size) {
  int var1 = 1;

  for(int var3 = 0; var3 < size; ++var3) {
      var1 = 31 * var1 + bytes[var3];
  }

  return var1;
}

JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_cache_NativeHashIndexReader_doNativeSearch
  (JNIEnv *env, jobject this, jlong mmAddr, jlong mmSize, jlong blockId, jshort rowGroupId, jshort columnId, jlong retAddr_) {
  // static variable is not thread-safe!!!
  // static char* retAddr = NULL;
  // if (!retAddr) {
  //   retAddr = (*env)->GetDirectBufferAddress(env, retBuf);
  // }
  // char* retAddr = (*env)->GetDirectBufferAddress(env, retBuf);
  char* retAddr = (char*) retAddr_;
  *(long *)retAddr = -1;
  MemoryMappedFile indexFile = (MemoryMappedFile){(char *)mmAddr, mmSize};

  char key[KEY_LEN] = {0};
  char zeros[KV_SIZE] = {0};
  memcpy(key, &blockId, sizeof(blockId));
  memcpy(key + sizeof(blockId), &rowGroupId, sizeof(rowGroupId));
  memcpy(key + sizeof(blockId) + sizeof(rowGroupId), &columnId, sizeof(columnId));

  int tableSize = (int) GET_LONG(indexFile, 16);

  int hash = hashcode(key, KEY_LEN) & 0x7fffffff;
  int bucket = hash % tableSize; // initial bucket
  int offset = bucket * KV_SIZE;
  const char* kv = GET_BYTES(indexFile, offset + INDEX_HASH_HEAD_OFFSET);
  int cmp = memcmp(kv, key, KEY_LEN);
  for (int i = 1; cmp != 0; ++i) {
    // quadratic probing
    bucket += i * i;
    bucket %= tableSize;
    offset = bucket * KV_SIZE;
    kv = GET_BYTES(indexFile, offset + INDEX_HASH_HEAD_OFFSET);
    if (memcmp(kv, zeros, KV_SIZE) == 0) {
      printf("cache miss! blk=%ld, rg=%d, col=%d, probe_i=%d, bucket=%d, offset=%d\n", blockId, rowGroupId, columnId, i, bucket, offset);
      return;
    }
    // check if key matches
    cmp = memcmp(kv, key, KEY_LEN);
  }
  memcpy(retAddr, kv + 12, 8); // offset
  memcpy((char*) retAddr + 8, kv + 20, 4); // length
}
