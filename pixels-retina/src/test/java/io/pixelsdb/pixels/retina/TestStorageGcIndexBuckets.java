package io.pixelsdb.pixels.retina;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.index.*;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.utils.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.index.IndexProto;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import org.junit.Test;
import static org.junit.Assert.*;
public class TestStorageGcIndexBuckets {
 @Test public void rewriteUsesIndexBucketsAndKeepsRollbackAlignedBeforePartialFailure() throws Exception {
  ConfigFactory.Instance().addProperty("index.bucket.num","16");
  Path dir=Files.createTempDirectory("gc-index-buckets-");
  ConfigFactory.Instance().addProperty("retina.storage.gc.journal.dir",dir.toUri().toString());
  StorageGcWal wal=new StorageGcWal();
  List<StorageGarbageCollector.PendingIndexEntry> pending=new ArrayList<>();
  for(int i=0;i<100;i++)pending.add(new StorageGarbageCollector.PendingIndexEntry(i,ByteString.copyFrom(ByteBuffer.allocate(4).putInt(i).array()),10));
  StorageGarbageCollector.RewriteResult result=new StorageGarbageCollector.RewriteResult(new StorageGarbageCollector.FileGroup(17,3,Collections.emptyList()),"file:///new.pxl",55,1,new int[]{100},new int[]{0,100},Collections.emptyMap(),Collections.emptyList(),pending);
  result.walWriter=wal.createTask("test",17,3,Collections.singletonList(44L),55,"file:///new.pxl",1000,100);
  final int[] updates={0};
  IndexService index=(IndexService)Proxy.newProxyInstance(IndexService.class.getClassLoader(),new Class[]{IndexService.class},(proxy,method,args)->{
   IndexOption option=(IndexOption)args[3];
   if(method.getName().equals("resolvePrimary")){
    List<Optional<ResolvedPrimary>> out=new ArrayList<>();
    for(IndexProto.IndexKey key:(List<IndexProto.IndexKey>)args[2]){
     assertEquals(IndexUtils.getBucketIdFromByteBuffer(key.getKey()),option.getVNodeId());
     int n=key.getKey().asReadOnlyByteBuffer().getInt();out.add(Optional.of(new ResolvedPrimary(100+n,IndexProto.RowLocation.newBuilder().setFileId(44).setRgRowOffset(n).build())));
    }return out;
   }
   if(method.getName().equals("updatePrimaryIndexEntriesOnly")){
    assertEquals(100,wal.getTask("test").get().getRollbackEntries().size());
    for(IndexProto.PrimaryIndexEntry entry:(List<IndexProto.PrimaryIndexEntry>)args[2])assertEquals(IndexUtils.getBucketIdFromByteBuffer(entry.getIndexKey().getKey()),option.getVNodeId());
    if(++updates[0]==2)throw new io.pixelsdb.pixels.common.exception.IndexException("Injected partial failure");
    return true;
   }
   throw new AssertionError(method.getName());
  });
  StorageGarbageCollector gc=new StorageGarbageCollector(null,null,index,.5,1024,20,10,100,EncodingLevel.from(2),0,wal);
  Method update=StorageGarbageCollector.class.getDeclaredMethod("updateSinglePointIndex",StorageGarbageCollector.RewriteResult.class,long.class,long.class,long.class);update.setAccessible(true);
  try{update.invoke(gc,result,17L,16L,1000L);fail("Expected injected failure");}
  catch(InvocationTargetException e){assertTrue(e.getCause() instanceof io.pixelsdb.pixels.common.exception.IndexException);}
  assertEquals(2,updates[0]);assertEquals(100,result.oldRowIds.size());
  List<RollbackEntry> rollback=wal.getTask("test").get().getRollbackEntries();
  for(int i=0;i<100;i++){assertEquals(100L+i,(long)result.oldRowIds.get(i));assertEquals(100L+i,rollback.get(i).getOldRowId());assertEquals(1000L+i,rollback.get(i).getNewRowId());}
  int restored=0;
  for(Map.Entry<Integer,List<RollbackEntry>> bucket:StorageGarbageCollector.indexRollbackBuckets(rollback).entrySet())for(RollbackEntry entry:bucket.getValue()){
   assertEquals(IndexUtils.getBucketIdFromByteBuffer(entry.getIndexKey().getKey()),(int)bucket.getKey());restored++;
  }
  assertEquals(100,restored);result.walWriter.close();
 }
}
