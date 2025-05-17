package io.pixelsdb.pixels.common.index;

import com.google.protobuf.ByteString;


import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
//import io.etcd.jetcd.api.KeyValue;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito.*;

public class MainIndexImplTest {

    @Mock
    private EtcdUtil mockedEtcdUtil;
    private MainIndexImpl mainIndex;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        // 使用反射注入mock的EtcdUtil实例
        mainIndex = new MainIndexImpl();
        TestUtils.setPrivateField(mainIndex, "etcdUtil", mockedEtcdUtil);
    }

    // 测试辅助方法
    private RowIdRange createRange(long start, long end) {
        return new RowIdRange(start, end);
    }

    private MainIndex.RgLocation createLocation(int fileId, int rgId) {
        return new MainIndex.RgLocation(fileId, rgId);
    }

    // 测试用例开始
    @Test
    public void testGetLocation_ValidRowId() {
        // 准备测试数据
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        mainIndex.putRowIdsOfRg(createRange(300, 400), createLocation(1, 2));

        // 测试边界值
        IndexProto.RowLocation location1 = mainIndex.getLocation(100);
        Assertions.assertNotNull(location1);
        Assertions.assertEquals(1, location1.getFileId());
        Assertions.assertEquals(1, location1.getRgId());
        Assertions.assertEquals(0, location1.getRgRowId());  // 100-100=0

        // 测试中间值
        IndexProto.RowLocation location2 = mainIndex.getLocation(350);
        Assertions.assertNotNull(location2);
        Assertions.assertEquals(1, location2.getFileId());
        Assertions.assertEquals(2, location2.getRgId());
        Assertions.assertEquals(50, location2.getRgRowId());  // 350-300=50

        // 测试不存在的rowId
        Assertions.assertNull(mainIndex.getLocation(500));
    }

    @Test
    public void testGetLocation_EmptyIndex() {
        Assertions.assertNull(mainIndex.getLocation(100));
    }

    @Test
    public void testPutRowIdsOfRg_Success() {
        Assertions.assertTrue(mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1)));
        Assertions.assertTrue(mainIndex.putRowIdsOfRg(createRange(300, 400), createLocation(1, 2)));

        // 验证entries已排序
        List<MainIndexImpl.Entry> entries = TestUtils.getPrivateField(mainIndex, "entries");
        Assertions.assertEquals(100L, entries.get(0).getRowIdRange().getStartRowId());
        Assertions.assertEquals(300L, entries.get(1).getRowIdRange().getStartRowId());
    }

    @Test
    public void testPutRowIdsOfRg_OverlappingRanges() {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));

        // 完全重叠
        Assertions.assertFalse(mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 2)));

        // 部分重叠
        Assertions.assertFalse(mainIndex.putRowIdsOfRg(createRange(150, 250), createLocation(1, 3)));

        // 包含
        Assertions.assertFalse(mainIndex.putRowIdsOfRg(createRange(50, 250), createLocation(1, 4)));
    }

    @Test
    public void testPutRowIdsOfRg_DirtyFlag() {
        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertFalse(isDirty);
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertTrue(isDirty);
    }

    @Test
    public void testDeleteRowIdRange_Success() {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        mainIndex.putRowIdsOfRg(createRange(300, 400), createLocation(1, 2));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(createRange(100, 200)));

        List<MainIndexImpl.Entry> entries = TestUtils.getPrivateField(mainIndex, "entries");
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(300L, entries.get(0).getRowIdRange().getStartRowId());

        // 验证dirty标志
        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertTrue(isDirty);
    }

    @Test
    public void testDeleteRowIdRange_NoMatch() {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(createRange(300, 400)));

        // 验证entries未变化
        List<MainIndexImpl.Entry> entries = TestUtils.getPrivateField(mainIndex, "entries");
        Assertions.assertEquals(1, entries.size());

//        // 验证dirty标志未变化
//        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
//        Assertions.assertFalse(isDirty);
    }

    @Test
    public void testGetRowId_Success() throws Exception {
        // 模拟从etcd加载数据
        List<KeyValue> mockKvs = new ArrayList<>();
        ByteSequence namespace = ByteSequence.EMPTY;
        io.etcd.jetcd.api.KeyValue kvs = io.etcd.jetcd.api.KeyValue.newBuilder().setKey(ByteString.copyFrom("/rowId/1001",StandardCharsets.UTF_8)).setValue(ByteString.copyFrom("value", StandardCharsets.UTF_8)).build();
        mockKvs.add(new KeyValue(kvs, namespace));
        Mockito.when(mockedEtcdUtil.getKeyValuesByPrefix("/rowId/")).thenReturn(mockKvs);

        // 测试
        SecondaryIndex.Entry mockEntry = Mockito.mock(SecondaryIndex.Entry.class);
        Assertions.assertTrue(mainIndex.getRowId(mockEntry));

        // 验证
        Mockito.verify(mockEntry).setRowId(1001L);
        Mockito.verify(mockedEtcdUtil).delete("/rowId/1001");
    }

    @Test
    public void testGetRowId_NoDataInEtcd() {
        Mockito.when(mockedEtcdUtil.getKeyValuesByPrefix("/rowId/")).thenReturn(new ArrayList<>());

        SecondaryIndex.Entry mockEntry = Mockito.mock(SecondaryIndex.Entry.class);
        Assertions.assertFalse(mainIndex.getRowId(mockEntry));
    }

    @Test
    public void testGetRgOfRowIds_Success() throws Exception {
        // 准备测试数据
        List<KeyValue> mockKvs = new ArrayList<>();
        ByteSequence namespace = ByteSequence.EMPTY;
        for (int i = 1; i <= 5; i++) {
            io.etcd.jetcd.api.KeyValue kvs = io.etcd.jetcd.api.KeyValue.newBuilder().setKey(ByteString.copyFrom("/rowId/100" + i,StandardCharsets.UTF_8)).setValue(ByteString.copyFrom("value", StandardCharsets.UTF_8)).build();
            mockKvs.add(new KeyValue(kvs, namespace));
        }
        Mockito.when(mockedEtcdUtil.getKeyValuesByPrefix("/rowId/")).thenReturn(mockKvs);

        // 准备3个entry
        List<SecondaryIndex.Entry> entries = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            entries.add(Mockito.mock(SecondaryIndex.Entry.class));
        }

        Assertions.assertTrue(mainIndex.getRgOfRowIds(entries));

        // 验证每个entry都设置了rowId
        for (SecondaryIndex.Entry entry : entries) {
            Mockito.verify(entry).setRowId(ArgumentMatchers.anyLong());
        }

        // 验证缓存中剩余2个rowId
        List<Long> cache = TestUtils.getPrivateField(mainIndex, "rowIdCache");
        Assertions.assertEquals(2, cache.size());
    }

    @Test
    public void testPersist_Success() {
        // 准备测试数据
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        mainIndex.putRowIdsOfRg(createRange(300, 400), createLocation(1, 2));

        // 模拟etcd操作成功
        Mockito.doNothing().when(mockedEtcdUtil).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        Assertions.assertTrue(mainIndex.persist());

        // 验证etcd被调用了2次
        Mockito.verify(mockedEtcdUtil, Mockito.times(2)).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    }

//    @Test
//    public void testPersist_Failure() {
//        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
//
//        // 模拟InterruptedException
//        Mockito.doThrow(new RuntimeException(new InterruptedException("Mocked interrupt")))
//                .when(mockedEtcdUtil).putKeyValue(ArgumentMatchers.eq("/error/key"), ArgumentMatchers.anyString());
//        // 模拟ExecutionException
//        Mockito.doThrow(new RuntimeException(new ExecutionException("Mocked etcd error", new RuntimeException())))
//                .when(mockedEtcdUtil).putKeyValue(ArgumentMatchers.eq("/fail/key"), ArgumentMatchers.anyString());
//
//        Assertions.assertFalse(mainIndex.persist());
//    }

    @Test
    public void testPersistIfDirty_WhenDirty() {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        Mockito.doNothing().when(mockedEtcdUtil).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        Assertions.assertTrue(mainIndex.persistIfDirty());
        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertFalse(isDirty);
    }

    @Test
    public void testPersistIfDirty_WhenNotDirty() {
        Assertions.assertTrue(mainIndex.persistIfDirty());
        Mockito.verify(mockedEtcdUtil, Mockito.never()).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    }

    @Test
    public void testClose_Success() throws IOException {
        // 数据是干净的，不需要持久化
        mainIndex.close();

        // 验证没有抛出异常
        Assertions.assertTrue(true);
    }

    @Test
    public void testClose_WithDirtyData() throws IOException {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        Mockito.doNothing().when(mockedEtcdUtil).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        mainIndex.close();

        // 验证dirty标志被清除
        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertFalse(isDirty);
    }

    @Test
    public void testClose_PersistFailure() {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        //模拟putKeyValue抛出异常（导致persist()失败）
        Mockito.doThrow(new RuntimeException("ETCD error"))
                .when(mockedEtcdUtil).putKeyValue(Mockito.anyString(), Mockito.anyString());

        // 3. 验证close()抛出IOException
        Assertions.assertThrows(IOException.class, () -> mainIndex.close());
    }
}