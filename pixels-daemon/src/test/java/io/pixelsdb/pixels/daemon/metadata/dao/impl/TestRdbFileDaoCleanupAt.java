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
package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
 * Mockito-based unit tests for the c01.1 changes in {@link RdbFileDao} that govern how the
 * optional {@code FILE_CLEANUP_AT} column is persisted and restored.
 *
 * <p>Contract under test:
 * <ul>
 *   <li>On INSERT / UPDATE: {@code FILE_CLEANUP_AT} is bound to a real {@code long} only when
 *       {@code type == RETIRED && hasCleanupAt()}.  Every other combination must bind {@code NULL}.</li>
 *   <li>On SELECT: a {@code wasNull()} column on the result set must materialise as
 *       {@code !proto.hasCleanupAt()} on the wire.</li>
 *   <li>{@code atomicSwapFiles} must clear {@code FILE_CLEANUP_AT} (set to NULL) when it
 *       promotes the new file to {@code REGULAR}, otherwise stale deadlines would leak across
 *       the swap boundary.</li>
 * </ul>
 *
 * <p>The DAO calls {@code MetaDBUtil.Instance().getConnection()} on every method, so we
 * inject a mock {@link Connection} into the singleton via reflection.  This keeps the test
 * a true unit test (no JDBC driver, no schema, no network).
 *
 * @author tdd-guide
 * @create 2026-05-13
 */
public class TestRdbFileDaoCleanupAt
{
    private static final int RETIRED_VALUE = MetadataProto.File.Type.RETIRED.getNumber();
    private static final int REGULAR_VALUE = MetadataProto.File.Type.REGULAR.getNumber();
    private static final int TEMPORARY_INGEST_VALUE =
            MetadataProto.File.Type.TEMPORARY_INGEST.getNumber();

    private Connection mockConn;
    private Connection originalConn;

    private RdbFileDao dao;

    @Before
    public void setUp() throws Exception
    {
        mockConn = mock(Connection.class);
        // The DAO does conn.isValid(30) on lazy reconnect; force it to report healthy so the
        // production code path stays on our mock rather than re-acquiring a real connection.
        when(mockConn.isValid(anyInt())).thenReturn(true);

        originalConn = swapConnection(mockConn);
        dao = new RdbFileDao();
    }

    @After
    public void tearDown() throws Exception
    {
        // Always restore the real connection so subsequent tests in the same JVM are unaffected.
        swapConnection(originalConn);
    }

    // -------------------------------------------------------------------------
    // INSERT — single row
    // -------------------------------------------------------------------------

    /**
     * For non-RETIRED file types (REGULAR, TEMPORARY_INGEST, TEMPORARY_GC), the DAO must NOT persist a cleanup deadline,
     * even if a stray {@code cleanupAt} happens to be present on the proto.
     */
    @Test
    public void insert_nonRetiredFileTypes_bindCleanupAtAsNull() throws Exception
    {
        // Test REGULAR file with stray cleanupAt value
        PreparedStatement pst1 = stubPreparedStatementForInsert();
        MetadataProto.File regularFile = baseFile("a.pxl", REGULAR_VALUE)
                .setCleanupAt(123_456_789L)  // deliberately stray; type != RETIRED so MUST be ignored
                .build();
        dao.insert(regularFile);
        verify(pst1).setNull(7, Types.BIGINT);
        verify(pst1, never()).setLong(eq(7), anyLong());

        // Test TEMPORARY_INGEST file (no cleanupAt)
        PreparedStatement pst2 = stubPreparedStatementForInsert();
        MetadataProto.File ingestFile = baseFile("ingest.pxl", TEMPORARY_INGEST_VALUE).build();
        dao.insert(ingestFile);
        verify(pst2).setNull(7, Types.BIGINT);
        verify(pst2, never()).setLong(eq(7), anyLong());
    }

    /**
     * RETIRED file binding tests covering various cleanupAt scenarios
     */
    @Test
    public void insert_retiredFile_bindingScenarios() throws Exception
    {
        // Test RETIRED file with cleanup deadline
        PreparedStatement pst1 = stubPreparedStatementForInsert();
        long deadline = 1_700_000_000_000L;
        MetadataProto.File retiredWithDeadline = baseFile("retired.pxl", RETIRED_VALUE)
                .setCleanupAt(deadline)
                .build();
        dao.insert(retiredWithDeadline);
        verify(pst1).setLong(7, deadline);
        verify(pst1, never()).setNull(eq(7), anyInt());

        // Test RETIRED file without cleanupAt (should bind NULL)
        PreparedStatement pst2 = stubPreparedStatementForInsert();
        MetadataProto.File retiredNoDeadline = baseFile("retired_unset.pxl", RETIRED_VALUE).build();
        dao.insert(retiredNoDeadline);
        verify(pst2).setNull(7, Types.BIGINT);
        verify(pst2, never()).setLong(eq(7), anyLong());

        // Test RETIRED file with cleanupAt = 0L (should bind as long zero, not NULL)
        PreparedStatement pst3 = stubPreparedStatementForInsert();
        MetadataProto.File retiredZero = baseFile("retired_zero.pxl", RETIRED_VALUE)
                .setCleanupAt(0L)
                .build();
        dao.insert(retiredZero);
        verify(pst3).setLong(7, 0L);
        verify(pst3, never()).setNull(eq(7), anyInt());
    }

    // -------------------------------------------------------------------------
    // INSERT BATCH — verifies per-row binding semantics
    // -------------------------------------------------------------------------

    @Test
    public void insertBatch_mixedTypes_bindsCleanupAtPerRow() throws Exception
    {
        PreparedStatement pst = stubPreparedStatementForInsert();

        MetadataProto.File regular = baseFile("r.pxl", REGULAR_VALUE).build();
        MetadataProto.File retiredWithDeadline = baseFile("d.pxl", RETIRED_VALUE)
                .setCleanupAt(42L).build();
        MetadataProto.File retiredNoDeadline = baseFile("nd.pxl", RETIRED_VALUE).build();

        dao.insertBatch(Arrays.asList(regular, retiredWithDeadline, retiredNoDeadline));

        // Two rows must bind NULL (regular + retired-without-deadline), one row binds a long.
        verify(pst, times(2)).setNull(7, Types.BIGINT);
        verify(pst, times(1)).setLong(7, 42L);
        verify(pst).executeBatch();
    }

    // -------------------------------------------------------------------------
    // UPDATE — index 6 carries cleanupAt (id is bound at index 7)
    // -------------------------------------------------------------------------

    /**
     * UPDATE operation binding tests for different file types and cleanupAt scenarios
     */
    @Test
    public void update_bindingScenarios() throws Exception
    {
        // Test REGULAR file - should bind cleanupAt as NULL
        PreparedStatement pst1 = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst1);
        when(pst1.executeUpdate()).thenReturn(1);

        MetadataProto.File regularFile = baseFile("u.pxl", REGULAR_VALUE).setId(7L).build();
        boolean ok1 = dao.update(regularFile);

        assertTrue(ok1);
        verify(pst1).setNull(6, Types.BIGINT);
        verify(pst1).setLong(7, 7L);  // WHERE FILE_ID = ?

        // Test RETIRED file with cleanup deadline - should bind as long
        PreparedStatement pst2 = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst2);
        when(pst2.executeUpdate()).thenReturn(1);

        long deadline = 1_700_000_000_999L;
        MetadataProto.File retiredFile = baseFile("u.pxl", RETIRED_VALUE)
                .setId(8L)
                .setCleanupAt(deadline)
                .build();
        boolean ok2 = dao.update(retiredFile);

        assertTrue(ok2);
        verify(pst2).setLong(6, deadline);
        verify(pst2).setLong(7, 8L);
    }

    // -------------------------------------------------------------------------
    // atomicSwapFiles — cleanupAt must be reset to NULL on promote
    // -------------------------------------------------------------------------

    /**
     * The promote step must use the SQL fragment {@code FILE_CLEANUP_AT=NULL}.  Without it,
     * a file that was previously RETIRED and is being recycled into a fresh REGULAR slot
     * would silently retain its deadline, eventually getting GC'd while live.
     */
    @Test
    public void atomicSwapFiles_promoteSqlClearsCleanupAt() throws Exception
    {
        PreparedStatement updatePst = mock(PreparedStatement.class);
        PreparedStatement deletePst = mock(PreparedStatement.class);

        when(mockConn.prepareStatement(anyString())).thenAnswer(inv -> {
            String sql = inv.getArgument(0);
            if (sql.startsWith("UPDATE"))
            {
                return updatePst;
            }
            if (sql.startsWith("DELETE"))
            {
                return deletePst;
            }
            return mock(PreparedStatement.class);
        });

        boolean ok = dao.atomicSwapFiles(101L, Arrays.asList(11L, 12L));
        assertTrue(ok);

        // Capture the actual SQL string the production code sent to the JDBC driver.
        org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
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

        // The promote binds REGULAR + the new id, then commits.  These behaviours are tied
        // to the same transaction as the DELETE, so we check both ran on the same connection.
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
        when(mockConn.prepareStatement(anyString()))
                .thenThrow(new SQLException("boom"));

        boolean ok = dao.atomicSwapFiles(1L, Collections.singletonList(2L));

        assertFalse("atomicSwapFiles must report failure when the JDBC layer throws", ok);
        verify(mockConn).setAutoCommit(false);
        verify(mockConn).rollback();
        verify(mockConn).setAutoCommit(true);  // finally block must restore auto-commit
        verify(mockConn, never()).commit();
    }

    // -------------------------------------------------------------------------
    // SELECT (buildFile) — cleanupAt round-trip from ResultSet to proto
    // -------------------------------------------------------------------------

    /**
     * SELECT operation tests covering different cleanupAt scenarios from ResultSet to proto
     */
    @Test
    public void getById_cleanupAtRoundTripScenarios() throws Exception
    {
        // Test scenario 1: ResultSet with cleanupAt value (non-NULL)
        Statement st1 = mock(Statement.class);
        ResultSet rs1 = mock(ResultSet.class);
        when(mockConn.createStatement()).thenReturn(st1);
        when(st1.executeQuery(anyString())).thenReturn(rs1);
        when(rs1.next()).thenReturn(true).thenReturn(false);

        when(rs1.getLong("FILE_ID")).thenReturn(99L);
        when(rs1.getString("FILE_NAME")).thenReturn("x.pxl");
        when(rs1.getInt("FILE_TYPE")).thenReturn(RETIRED_VALUE);
        when(rs1.getInt("FILE_NUM_RG")).thenReturn(2);
        when(rs1.getLong("FILE_MIN_ROW_ID")).thenReturn(0L);
        when(rs1.getLong("FILE_MAX_ROW_ID")).thenReturn(127L);
        when(rs1.getLong("PATHS_PATH_ID")).thenReturn(5L);
        when(rs1.getLong("FILE_CLEANUP_AT")).thenReturn(1_700_000_000_000L);
        when(rs1.wasNull()).thenReturn(false);

        MetadataProto.File proto1 = dao.getById(99L);

        assertNotNull(proto1);
        assertEquals(99L, proto1.getId());
        assertEquals(MetadataProto.File.Type.RETIRED, proto1.getType());
        assertTrue("non-NULL FILE_CLEANUP_AT column must surface as hasCleanupAt()",
                proto1.hasCleanupAt());
        assertEquals(1_700_000_000_000L, proto1.getCleanupAt());

        // Test scenario 2: ResultSet with NULL cleanupAt
        Statement st2 = mock(Statement.class);
        ResultSet rs2 = mock(ResultSet.class);
        when(mockConn.createStatement()).thenReturn(st2);
        when(st2.executeQuery(anyString())).thenReturn(rs2);
        when(rs2.next()).thenReturn(true).thenReturn(false);

        when(rs2.getLong("FILE_ID")).thenReturn(1L);
        when(rs2.getString("FILE_NAME")).thenReturn("r.pxl");
        when(rs2.getInt("FILE_TYPE")).thenReturn(REGULAR_VALUE);
        when(rs2.getInt("FILE_NUM_RG")).thenReturn(1);
        when(rs2.getLong("FILE_MIN_ROW_ID")).thenReturn(0L);
        when(rs2.getLong("FILE_MAX_ROW_ID")).thenReturn(0L);
        when(rs2.getLong("PATHS_PATH_ID")).thenReturn(1L);
        when(rs2.getLong("FILE_CLEANUP_AT")).thenReturn(0L);
        when(rs2.wasNull()).thenReturn(true);  // critical: NULL column

        MetadataProto.File proto2 = dao.getById(1L);

        assertNotNull(proto2);
        assertFalse("NULL FILE_CLEANUP_AT column must surface as !hasCleanupAt()",
                proto2.hasCleanupAt());
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private PreparedStatement stubPreparedStatementForInsert() throws SQLException
    {
        PreparedStatement pst = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(pst);
        when(pst.executeUpdate()).thenReturn(1);

        // After a successful insert, the DAO calls executeQuery("SELECT LAST_INSERT_ID()")
        // on the same PreparedStatement.  Stub a single-row ResultSet so the call returns cleanly.
        ResultSet idRs = mock(ResultSet.class);
        when(pst.executeQuery(anyString())).thenReturn(idRs);
        when(idRs.next()).thenReturn(true);
        when(idRs.getLong(1)).thenReturn(1L);
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

    /**
     * Replace the private {@code connection} field in the {@link MetaDBUtil} singleton with
     * the supplied connection, returning the previous value.  Using reflection here keeps
     * the production class untouched while still letting us inject a Mockito-managed
     * {@link Connection} for the duration of a single test.
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
