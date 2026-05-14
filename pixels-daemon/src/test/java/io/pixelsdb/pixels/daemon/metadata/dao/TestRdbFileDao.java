/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.impl.RdbFileDao;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RdbFileDao} cleanup-at handling and typed file enumeration.
 */
public class TestRdbFileDao
{
    private static final MetadataProto.File.Type REGULAR = MetadataProto.File.Type.REGULAR;
    private static final MetadataProto.File.Type RETIRED = MetadataProto.File.Type.RETIRED;
    private static final MetadataProto.File.Type TEMPORARY_INGEST =
            MetadataProto.File.Type.TEMPORARY_INGEST;
    private static final MetadataProto.File.Type TEMPORARY_GC =
            MetadataProto.File.Type.TEMPORARY_GC;

    private static final int REGULAR_VALUE = REGULAR.getNumber();
    private static final int RETIRED_VALUE = RETIRED.getNumber();
    private static final int TEMPORARY_INGEST_VALUE = TEMPORARY_INGEST.getNumber();
    private static final int TEMPORARY_GC_VALUE = TEMPORARY_GC.getNumber();

    private Connection mockConn;
    private Connection originalConn;
    private RdbFileDao dao;

    @Before
    public void setUp() throws Exception
    {
        mockConn = mock(Connection.class);
        // Keep lazy reconnect on the mock connection.
        when(mockConn.isValid(anyInt())).thenReturn(true);
        originalConn = swapConnection(mockConn);
        dao = new RdbFileDao();
    }

    @After
    public void tearDown() throws Exception
    {
        swapConnection(originalConn);
    }

    // =========================================================================
    // INSERT / UPDATE cleanup-at binding
    // =========================================================================

    /**
     * Non-RETIRED rows bind {@code FILE_CLEANUP_AT} as {@code NULL}.
     */
    @Test
    public void insert_nonRetired_withoutCleanupAt_bindsNull() throws Exception
    {
        PreparedStatement pstRegular = stubPreparedStatementForInsert();
        dao.insert(baseFile("a.pxl", REGULAR_VALUE).build());
        verify(pstRegular).setNull(7, Types.BIGINT);
        verify(pstRegular, never()).setLong(eq(7), anyLong());

        PreparedStatement pstIngest = stubPreparedStatementForInsert();
        dao.insert(baseFile("ingest_unset.pxl", TEMPORARY_INGEST_VALUE).build());
        verify(pstIngest).setNull(7, Types.BIGINT);
        verify(pstIngest, never()).setLong(eq(7), anyLong());

        PreparedStatement pstGc = stubPreparedStatementForInsert();
        dao.insert(baseFile("gc_unset.pxl", TEMPORARY_GC_VALUE).build());
        verify(pstGc).setNull(7, Types.BIGINT);
        verify(pstGc, never()).setLong(eq(7), anyLong());
    }

    /**
     * Non-RETIRED rows with {@code cleanupAt} are rejected before writing.
     */
    @Test
    public void insert_nonRetired_withCleanupAt_failsFast() throws Exception
    {
        PreparedStatement pst = stubPreparedStatementForInsert();
        long unwanted = 123_456_789L;
        long id = dao.insert(baseFile("a.pxl", REGULAR_VALUE).setCleanupAt(unwanted).build());
        assertEquals("DAO must surface the invariant violation as the -1 failure sentinel", -1L, id);
        verify(pst, never()).setLong(eq(7), anyLong());
        verify(pst, never()).setNull(eq(7), anyInt());
        verify(pst, never()).executeUpdate();

        PreparedStatement pst2 = stubPreparedStatementForInsert();
        long id2 = dao.insert(baseFile("t.pxl", TEMPORARY_GC_VALUE).setCleanupAt(24L).build());
        assertEquals(-1L, id2);
        verify(pst2, never()).executeUpdate();
    }

    /**
     * RETIRED rows bind the provided cleanup deadline.
     */
    @Test
    public void insert_retiredFile_bindingScenarios() throws Exception
    {
        PreparedStatement pst1 = stubPreparedStatementForInsert();
        long deadline = 1_700_000_000_000L;
        dao.insert(baseFile("retired.pxl", RETIRED_VALUE).setCleanupAt(deadline).build());
        verify(pst1).setLong(7, deadline);
        verify(pst1, never()).setNull(eq(7), anyInt());

        PreparedStatement pst2 = stubPreparedStatementForInsert();
        dao.insert(baseFile("retired_zero.pxl", RETIRED_VALUE).setCleanupAt(0L).build());
        verify(pst2).setLong(7, 0L);
        verify(pst2, never()).setNull(eq(7), anyInt());
    }

    /**
     * RETIRED rows without {@code cleanupAt} are rejected.
     */
    @Test
    public void insert_retired_withoutCleanupAt_failsFast() throws Exception
    {
        PreparedStatement pst = stubPreparedStatementForInsert();
        long id = dao.insert(baseFile("nd.pxl", RETIRED_VALUE).build());
        assertEquals(-1L, id);
        verify(pst, never()).executeUpdate();
    }

    @Test
    public void insertBatch_mixedTypes_bindsCleanupAtPerRow() throws Exception
    {
        PreparedStatement pst = stubPreparedStatementForInsert();

        MetadataProto.File regular = baseFile("r.pxl", REGULAR_VALUE).build();
        MetadataProto.File temporaryNoDeadline = baseFile("t.pxl", TEMPORARY_GC_VALUE).build();
        MetadataProto.File ingestNoDeadline = baseFile("i.pxl", TEMPORARY_INGEST_VALUE).build();
        MetadataProto.File retiredWithDeadline = baseFile("d.pxl", RETIRED_VALUE)
                .setCleanupAt(42L).build();

        assertTrue(dao.insertBatch(
                Arrays.asList(regular, temporaryNoDeadline, ingestNoDeadline, retiredWithDeadline)));

        // Three non-RETIRED rows bind NULL; the single RETIRED row binds its deadline.
        verify(pst, times(3)).setNull(7, Types.BIGINT);
        verify(pst, times(1)).setLong(7, 42L);
        verify(pst).executeBatch();
    }

    /**
     * Any invalid cleanup-at row rejects the whole batch.
     */
    @Test
    public void insertBatch_invariantViolation_rejectsWholeBatch() throws Exception
    {
        PreparedStatement pst = stubPreparedStatementForInsert();

        // Mix one legal RETIRED with one illegal TEMPORARY_GC+cleanupAt.
        MetadataProto.File legal = baseFile("d.pxl", RETIRED_VALUE).setCleanupAt(42L).build();
        MetadataProto.File illegal = baseFile("t.pxl", TEMPORARY_GC_VALUE).setCleanupAt(24L).build();

        assertFalse(dao.insertBatch(Arrays.asList(legal, illegal)));
        verify(pst, never()).executeBatch();
    }

    /**
     * UPDATE binds cleanup-at at index 6 and the WHERE id at index 7.
     */
    @Test
    public void update_bindingScenarios() throws Exception
    {
        PreparedStatement pst1 = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst1);
        when(pst1.executeUpdate()).thenReturn(1);
        assertTrue(dao.update(baseFile("u.pxl", REGULAR_VALUE).setId(7L).build()));
        verify(pst1).setNull(6, Types.BIGINT);
        verify(pst1).setLong(7, 7L);

        PreparedStatement pst2 = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst2);
        when(pst2.executeUpdate()).thenReturn(1);
        long deadline = 1_700_000_000_999L;
        assertTrue(dao.update(baseFile("u.pxl", RETIRED_VALUE).setId(8L)
                .setCleanupAt(deadline).build()));
        verify(pst2).setLong(6, deadline);
        verify(pst2).setLong(7, 8L);
    }

    /**
     * Invalid cleanup-at combinations are rejected on UPDATE.
     */
    @Test
    public void update_invariantViolations_failFast() throws Exception
    {
        PreparedStatement pst1 = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst1);
        assertFalse(dao.update(baseFile("u.pxl", TEMPORARY_GC_VALUE).setId(8L)
                .setCleanupAt(99L).build()));
        verify(pst1, never()).executeUpdate();

        PreparedStatement pst2 = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst2);
        assertFalse(dao.update(baseFile("u.pxl", RETIRED_VALUE).setId(9L).build()));
        verify(pst2, never()).executeUpdate();
    }

    // =========================================================================
    // atomicSwapFiles transactional behaviour
    // =========================================================================

    /**
     * Promoting a file clears {@code FILE_CLEANUP_AT} with the type update.
     */
    @Test
    public void atomicSwapFiles_promoteSqlClearsCleanupAt() throws Exception
    {
        PreparedStatement updatePst = mock(PreparedStatement.class);
        PreparedStatement deletePst = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenAnswer(inv -> {
            String sql = inv.getArgument(0);
            if (sql.startsWith("UPDATE")) return updatePst;
            if (sql.startsWith("DELETE")) return deletePst;
            return mock(PreparedStatement.class);
        });

        assertTrue(dao.atomicSwapFiles(101L, Arrays.asList(11L, 12L)));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConn, atLeastOnce()).prepareStatement(sqlCaptor.capture());
        boolean clearsCleanupAt = false;
        for (String sql : sqlCaptor.getAllValues())
        {
            if (sql.contains("FILE_TYPE=?") && sql.contains("FILE_CLEANUP_AT=NULL"))
            {
                clearsCleanupAt = true;
                break;
            }
        }
        assertTrue("promote SQL must clear FILE_CLEANUP_AT to NULL together with the type update",
                clearsCleanupAt);

        verify(updatePst).setInt(1, REGULAR_VALUE);
        verify(updatePst).setLong(2, 101L);
        verify(updatePst).executeUpdate();
        verify(deletePst).setLong(1, 11L);
        verify(deletePst).setLong(2, 12L);
        verify(deletePst).executeUpdate();
        verify(mockConn).setAutoCommit(false);
        verify(mockConn).commit();
    }

    @Test
    public void atomicSwapFiles_rollsBackOnSqlException() throws Exception
    {
        when(mockConn.prepareStatement(anyString())).thenThrow(new SQLException("boom"));

        assertFalse("atomicSwapFiles must report failure when the JDBC layer throws",
                dao.atomicSwapFiles(1L, Collections.singletonList(2L)));
        verify(mockConn).setAutoCommit(false);
        verify(mockConn).rollback();
        verify(mockConn).setAutoCommit(true);
        verify(mockConn, never()).commit();
    }

    // =========================================================================
    // SELECT cleanup-at round-trip
    // =========================================================================

    /**
     * SQL {@code NULL} cleanup-at values surface as unset proto fields.
     */
    @Test
    public void getById_cleanupAtRoundTripScenarios() throws Exception
    {
        // Scenario 1: non-NULL deadline must surface as hasCleanupAt() == true
        Statement st1 = mock(Statement.class);
        ResultSet rs1 = mock(ResultSet.class);
        when(mockConn.createStatement()).thenReturn(st1);
        when(st1.executeQuery(anyString())).thenReturn(rs1);
        when(rs1.next()).thenReturn(true).thenReturn(false);
        stubFileRow(rs1, 99L, "x.pxl", RETIRED_VALUE, 5L, 1_700_000_000_000L, /*wasNull*/ false);

        MetadataProto.File proto1 = dao.getById(99L);
        assertNotNull(proto1);
        assertEquals(99L, proto1.getId());
        assertEquals(RETIRED, proto1.getType());
        assertTrue("non-NULL FILE_CLEANUP_AT column must surface as hasCleanupAt()",
                proto1.hasCleanupAt());
        assertEquals(1_700_000_000_000L, proto1.getCleanupAt());

        // Scenario 2: NULL column must surface as !hasCleanupAt()
        Statement st2 = mock(Statement.class);
        ResultSet rs2 = mock(ResultSet.class);
        when(mockConn.createStatement()).thenReturn(st2);
        when(st2.executeQuery(anyString())).thenReturn(rs2);
        when(rs2.next()).thenReturn(true).thenReturn(false);
        stubFileRow(rs2, 1L, "r.pxl", REGULAR_VALUE, 1L, 0L, /*wasNull*/ true);

        MetadataProto.File proto2 = dao.getById(1L);
        assertNotNull(proto2);
        assertFalse("NULL FILE_CLEANUP_AT column must surface as !hasCleanupAt()",
                proto2.hasCleanupAt());
    }

    // =========================================================================
    // getFilesByType
    // =========================================================================

    /**
     * Single-path queries bind path id first, then requested file types.
     */
    @Test
    public void getFilesByType_singlePath_bindsPathIdAndRequestedTypes() throws Exception
    {
        PreparedStatement pst = stubEmptyQuery();

        dao.getFilesByType(9L, Arrays.asList(TEMPORARY_INGEST, RETIRED));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConn).prepareStatement(sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        assertTrue("single-path enumeration must filter by PATHS_PATH_ID",
                sql.contains("PATHS_PATH_ID = ?"));
        assertTrue("enumeration must filter by FILE_TYPE IN (...)",
                sql.contains("FILE_TYPE IN ("));
        assertTrue("enumeration must order by FILE_ID for stable iteration",
                sql.contains("ORDER BY FILE_ID"));

        verify(pst).setLong(1, 9L);
        verify(pst).setInt(2, TEMPORARY_INGEST_VALUE);
        verify(pst).setInt(3, RETIRED_VALUE);
    }

    /**
     * Cross-path queries omit the path predicate and bind types from index 1.
     */
    @Test
    public void getFilesByType_crossPath_omitsPathPredicateAndBindsTypesAtIndexOne()
            throws Exception
    {
        PreparedStatement pst = stubEmptyQuery();

        dao.getFilesByType(/*pathId*/ null, Arrays.asList(TEMPORARY_INGEST, TEMPORARY_GC));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConn).prepareStatement(sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        assertFalse("cross-path enumeration must NOT include the PATHS_PATH_ID predicate",
                sql.contains("PATHS_PATH_ID"));
        assertTrue("cross-path enumeration must still filter by FILE_TYPE IN (...)",
                sql.contains("FILE_TYPE IN ("));
        assertTrue("cross-path enumeration must order by FILE_ID",
                sql.contains("ORDER BY FILE_ID"));

        // No path bind — type numbers start at index 1.
        verify(pst, never()).setLong(eq(1), anyLong());
        verify(pst).setInt(1, TEMPORARY_INGEST_VALUE);
        verify(pst).setInt(2, TEMPORARY_GC_VALUE);
    }

    /**
     * Repeated file types share one SQL placeholder.
     */
    @Test
    public void getFilesByType_dedupesRepeatedTypes() throws Exception
    {
        PreparedStatement pst = stubEmptyQuery();

        dao.getFilesByType(2L, Arrays.asList(REGULAR, REGULAR, REGULAR));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConn).prepareStatement(sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        int inStart = sql.indexOf("FILE_TYPE IN (");
        int inEnd = sql.indexOf(")", inStart);
        String inClause = sql.substring(inStart, inEnd);
        assertEquals("duplicate types must be deduped to a single placeholder",
                1, countOccurrences(inClause, '?'));

        verify(pst).setLong(1, 2L);
        verify(pst).setInt(2, REGULAR_VALUE);
        verify(pst, never()).setInt(eq(3), anyInt());
    }

    /**
     * Empty or null type lists return an empty result without querying JDBC.
     */
    @Test
    public void getFilesByType_emptyTypes_returnsEmptyWithoutQuerying() throws Exception
    {
        // Single-path empty / null
        List<MetadataProto.File> emptyResult = dao.getFilesByType(5L, Collections.emptyList());
        assertNotNull(emptyResult);
        assertTrue(emptyResult.isEmpty());

        List<MetadataProto.File> nullResult = dao.getFilesByType(5L, null);
        assertNotNull(nullResult);
        assertTrue(nullResult.isEmpty());

        // Cross-path empty / null
        List<MetadataProto.File> crossEmpty = dao.getFilesByType(null, Collections.emptyList());
        assertNotNull(crossEmpty);
        assertTrue(crossEmpty.isEmpty());

        List<MetadataProto.File> crossNull = dao.getFilesByType(null, null);
        assertNotNull(crossNull);
        assertTrue(crossNull.isEmpty());

        verify(mockConn, never()).prepareStatement(anyString());
        verify(mockConn, never()).createStatement();
    }

    /**
     * SQL exceptions return {@code null} on single-path queries.
     */
    @Test
    public void getFilesByType_singlePath_sqlException_returnsNull() throws Exception
    {
        when(mockConn.prepareStatement(anyString())).thenThrow(new SQLException("boom"));

        List<MetadataProto.File> failure =
                dao.getFilesByType(7L, Collections.singletonList(REGULAR));
        assertNull("SQL exception on single-path enumeration must surface as null", failure);
    }

    /**
     * SQL exceptions return {@code null} on cross-path queries.
     */
    @Test
    public void getFilesByType_crossPath_sqlException_returnsNull() throws Exception
    {
        when(mockConn.prepareStatement(anyString())).thenThrow(new SQLException("boom"));

        List<MetadataProto.File> failure =
                dao.getFilesByType(null, Collections.singletonList(RETIRED));
        assertNull("SQL exception on cross-path enumeration must surface as null", failure);
    }

    // =========================================================================
    // deleteByIds
    // =========================================================================

    /**
     * deleteByIds batches {@code FILE_ID} deletes with one SQL template.
     */
    @Test
    public void deleteByIds_batchesBindsAndIssuesSingleSqlTemplate() throws Exception
    {
        PreparedStatement pst = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst);

        assertTrue(dao.deleteByIds(Arrays.asList(11L, 22L, 33L)));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConn).prepareStatement(sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        assertEquals("deleteByIds must use a positional FILE_ID=? template (batched)",
                "DELETE FROM FILES WHERE FILE_ID=?", sql);

        verify(pst).setLong(1, 11L);
        verify(pst).setLong(1, 22L);
        verify(pst).setLong(1, 33L);
        verify(pst, times(3)).addBatch();
        verify(pst).executeBatch();
    }

    // =========================================================================
    // helpers
    // =========================================================================

    private PreparedStatement stubPreparedStatementForInsert() throws SQLException
    {
        PreparedStatement pst = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst);
        when(pst.executeUpdate()).thenReturn(1);
        // Stub LAST_INSERT_ID() on the insert statement.
        ResultSet idRs = mock(ResultSet.class);
        when(pst.executeQuery(anyString())).thenReturn(idRs);
        when(idRs.next()).thenReturn(true);
        when(idRs.getLong(1)).thenReturn(1L);
        return pst;
    }

    private PreparedStatement stubEmptyQuery() throws SQLException
    {
        PreparedStatement pst = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst);
        when(pst.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        return pst;
    }

    private static MetadataProto.File.Builder baseFile(String name, int typeValue)
    {
        return MetadataProto.File.newBuilder()
                .setName(name)
                .setTypeValue(typeValue)
                .setNumRowGroup(1)
                .setMinRowId(0L)
                .setMaxRowId(0L)
                .setPathId(1L);
    }

    private static void stubFileRow(ResultSet rs, long id, String name, int typeValue,
                                    long pathId, long cleanupAt, boolean cleanupAtWasNull)
            throws SQLException
    {
        when(rs.getLong("FILE_ID")).thenReturn(id);
        when(rs.getString("FILE_NAME")).thenReturn(name);
        when(rs.getInt("FILE_TYPE")).thenReturn(typeValue);
        when(rs.getInt("FILE_NUM_RG")).thenReturn(1);
        when(rs.getLong("FILE_MIN_ROW_ID")).thenReturn(0L);
        when(rs.getLong("FILE_MAX_ROW_ID")).thenReturn(0L);
        when(rs.getLong("PATHS_PATH_ID")).thenReturn(pathId);
        when(rs.getLong("FILE_CLEANUP_AT")).thenReturn(cleanupAt);
        when(rs.wasNull()).thenReturn(cleanupAtWasNull);
    }

    private static int countOccurrences(String haystack, char needle)
    {
        int n = 0;
        for (int i = 0; i < haystack.length(); i++)
        {
            if (haystack.charAt(i) == needle) n++;
        }
        return n;
    }

    /**
     * Swap the {@link MetaDBUtil} singleton connection for this test.
     */
    private static Connection swapConnection(Connection replacement) throws Exception
    {
        Field f = MetaDBUtil.class.getDeclaredField("connection");
        f.setAccessible(true);
        Connection previous = (Connection) f.get(MetaDBUtil.Instance());
        f.set(MetaDBUtil.Instance(), replacement);
        return previous;
    }
}
