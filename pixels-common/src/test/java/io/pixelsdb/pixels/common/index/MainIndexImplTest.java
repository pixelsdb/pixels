/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.index;

import com.google.protobuf.ByteString;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.exception.RowIdException;
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
import org.mockito.ArgumentMatchers;

public class MainIndexImplTest
{
    @Mock
    private EtcdUtil mockedEtcdUtil;
    private MainIndexImpl mainIndex;

    @BeforeEach
    public void setUp()
    {
        MockitoAnnotations.openMocks(this);
        // Use reflection to inject mocked EtcdUtil instance
        mainIndex = new MainIndexImpl();
        TestUtils.setPrivateField(mainIndex, "etcdUtil", mockedEtcdUtil);
    }

    // Test helper methods
    private RowIdRange createRange(long start, long end)
    {
        return new RowIdRange(start, end);
    }

    private MainIndex.RgLocation createLocation(int fileId, int rgId)
    {
        return new MainIndex.RgLocation(fileId, rgId);
    }

    // Test cases begin
    @Test
    public void testGetLocation_ValidRowId()
    {
        // Prepare test data
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        mainIndex.putRowIdsOfRg(createRange(300, 400), createLocation(1, 2));

        // Test boundary values
        IndexProto.RowLocation location1 = mainIndex.getLocation(100);
        Assertions.assertNotNull(location1);
        Assertions.assertEquals(1, location1.getFileId());
        Assertions.assertEquals(1, location1.getRgId());
        Assertions.assertEquals(0, location1.getRgRowId());  // 100-100=0

        // Test middle value
        IndexProto.RowLocation location2 = mainIndex.getLocation(350);
        Assertions.assertNotNull(location2);
        Assertions.assertEquals(1, location2.getFileId());
        Assertions.assertEquals(2, location2.getRgId());
        Assertions.assertEquals(50, location2.getRgRowId());  // 350-300=50

        // Test non-existent rowId
        Assertions.assertNull(mainIndex.getLocation(500));
    }

    @Test
    public void testGetLocation_EmptyIndex()
    {
        Assertions.assertNull(mainIndex.getLocation(100));
    }

    @Test
    public void testPutRowIdsOfRg_Success()
    {
        Assertions.assertTrue(mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1)));
        Assertions.assertTrue(mainIndex.putRowIdsOfRg(createRange(300, 400), createLocation(1, 2)));

        // Verify entries are sorted
        List<MainIndexImpl.Entry> entries = TestUtils.getPrivateField(mainIndex, "entries");
        Assertions.assertEquals(100L, entries.get(0).getRowIdRange().getStartRowId());
        Assertions.assertEquals(300L, entries.get(1).getRowIdRange().getStartRowId());
    }

    @Test
    public void testPutRowIdsOfRg_OverlappingRanges()
    {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));

        // Complete overlap
        Assertions.assertFalse(mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 2)));

        // Partial overlap
        Assertions.assertFalse(mainIndex.putRowIdsOfRg(createRange(150, 250), createLocation(1, 3)));

        // Contains
        Assertions.assertFalse(mainIndex.putRowIdsOfRg(createRange(50, 250), createLocation(1, 4)));
    }

    @Test
    public void testPutRowIdsOfRg_DirtyFlag()
    {
        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertFalse(isDirty);
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertTrue(isDirty);
    }

    @Test
    public void testDeleteRowIdRange_Success()
    {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        mainIndex.putRowIdsOfRg(createRange(300, 400), createLocation(1, 2));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(createRange(100, 200)));

        List<MainIndexImpl.Entry> entries = TestUtils.getPrivateField(mainIndex, "entries");
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(300L, entries.get(0).getRowIdRange().getStartRowId());

        // Verify dirty flag
        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertTrue(isDirty);
    }

    @Test
    public void testDeleteRowIdRange_NoMatch()
    {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));

        Assertions.assertTrue(mainIndex.deleteRowIdRange(createRange(300, 400)));

        // Verify entries unchanged
        List<MainIndexImpl.Entry> entries = TestUtils.getPrivateField(mainIndex, "entries");
        Assertions.assertEquals(1, entries.size());
    }

    @Test
    public void testGetRowId_Success() throws Exception
    {
        // Mock etcd data loading
        List<KeyValue> mockKvs = new ArrayList<>();
        ByteSequence namespace = ByteSequence.EMPTY;
        io.etcd.jetcd.api.KeyValue kvs = io.etcd.jetcd.api.KeyValue.newBuilder().setKey(ByteString.copyFrom("/rowId/1001",StandardCharsets.UTF_8)).setValue(ByteString.copyFrom("value", StandardCharsets.UTF_8)).build();
        mockKvs.add(new KeyValue(kvs, namespace));
        Mockito.when(mockedEtcdUtil.getKeyValuesByPrefix("/rowId/")).thenReturn(mockKvs);

        // Test
        SecondaryIndex.Entry mockEntry = Mockito.mock(SecondaryIndex.Entry.class);
        Assertions.assertTrue(mainIndex.getRowId(mockEntry));

        // Verify
        Mockito.verify(mockEntry).setRowId(1001L);
        Mockito.verify(mockedEtcdUtil).delete("/rowId/1001");
    }

    @Test
    public void testGetRowId_NoDataInEtcd() throws RowIdException {
        Mockito.when(mockedEtcdUtil.getKeyValuesByPrefix("/rowId/")).thenReturn(new ArrayList<>());

        SecondaryIndex.Entry mockEntry = Mockito.mock(SecondaryIndex.Entry.class);
        Assertions.assertFalse(mainIndex.getRowId(mockEntry));
    }

    @Test
    public void testGetRgOfRowIds_Success() throws Exception
    {
        // Prepare test data
        List<KeyValue> mockKvs = new ArrayList<>();
        ByteSequence namespace = ByteSequence.EMPTY;
        for (int i = 1; i <= 5; i++)
        {
            io.etcd.jetcd.api.KeyValue kvs = io.etcd.jetcd.api.KeyValue.newBuilder().setKey(ByteString.copyFrom("/rowId/100" + i,StandardCharsets.UTF_8)).setValue(ByteString.copyFrom("value", StandardCharsets.UTF_8)).build();
            mockKvs.add(new KeyValue(kvs, namespace));
        }
        Mockito.when(mockedEtcdUtil.getKeyValuesByPrefix("/rowId/")).thenReturn(mockKvs);

        // Prepare 3 entries
        List<SecondaryIndex.Entry> entries = new ArrayList<>();
        for (int i = 0; i < 3; i++)
        {
            entries.add(Mockito.mock(SecondaryIndex.Entry.class));
        }

        Assertions.assertTrue(mainIndex.getRgOfRowIds(entries));

        // Verify each entry got a rowId
        for (SecondaryIndex.Entry entry : entries)
        {
            Mockito.verify(entry).setRowId(ArgumentMatchers.anyLong());
        }

        // Verify cache has 2 rowIds remaining
        List<Long> cache = TestUtils.getPrivateField(mainIndex, "rowIdCache");
        Assertions.assertEquals(2, cache.size());
    }

    @Test
    public void testPersist_Success()
    {
        // Prepare test data
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        mainIndex.putRowIdsOfRg(createRange(300, 400), createLocation(1, 2));

        // Mock successful etcd operation
        Mockito.doNothing().when(mockedEtcdUtil).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        Assertions.assertTrue(mainIndex.persist());

        // Verify etcd was called twice
        Mockito.verify(mockedEtcdUtil, Mockito.times(2)).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    }

    @Test
    public void testPersistIfDirty_WhenDirty()
    {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        Mockito.doNothing().when(mockedEtcdUtil).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        Assertions.assertTrue(mainIndex.persistIfDirty());
        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertFalse(isDirty);
    }

    @Test
    public void testPersistIfDirty_WhenNotDirty()
    {
        Assertions.assertTrue(mainIndex.persistIfDirty());
        Mockito.verify(mockedEtcdUtil, Mockito.never()).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    }

    @Test
    public void testClose_Success() throws IOException
    {
        // Data is clean, no need to persist
        mainIndex.close();

        // Verify no exception thrown
        Assertions.assertTrue(true);
    }

    @Test
    public void testClose_WithDirtyData() throws IOException
    {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        Mockito.doNothing().when(mockedEtcdUtil).putKeyValue(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        mainIndex.close();

        // Verify dirty flag is cleared
        boolean isDirty = TestUtils.getPrivateField(mainIndex, "dirty");
        Assertions.assertFalse(isDirty);
    }

    @Test
    public void testClose_PersistFailure()
    {
        mainIndex.putRowIdsOfRg(createRange(100, 200), createLocation(1, 1));
        // Mock putKeyValue throwing exception (causing persist() to fail)
        Mockito.doThrow(new RuntimeException("ETCD error"))
                .when(mockedEtcdUtil).putKeyValue(Mockito.anyString(), Mockito.anyString());

        // Verify close() throws IOException
        Assertions.assertThrows(IOException.class, () -> mainIndex.close());
    }
}