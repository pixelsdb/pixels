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
package io.pixelsdb.pixels.common.metadata.domain;

import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.daemon.MetadataProto;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link File} that exercise the c01.1 contract:
 * <ul>
 *   <li>{@link File.Type} now carries an explicit numeric tag (no longer relies on {@code ordinal()}).</li>
 *   <li>The four enum constants — {@code TEMPORARY_INGEST(0)}, {@code REGULAR(1)},
 *       {@code TEMPORARY_GC(2)}, {@code RETIRED(3)} — must round-trip cleanly through both
 *       {@link MetadataProto.File} and the domain object.</li>
 *   <li>{@link File#getCleanupAt()} is an optional field: it must be preserved across
 *       {@link File#toProto()} / {@code new File(MetadataProto.File)} when present and absent.</li>
 * </ul>
 *
 * @author tdd-guide
 * @create 2026-05-13
 */
public class TestFileDomain
{
    // -------------------------------------------------------------------------
    // File.Type — numeric tags
    // -------------------------------------------------------------------------

    /**
     * The domain {@link File.Type#getNumber()} must agree with the proto-generated
     * {@link MetadataProto.File.Type#getNumber()} for every constant we publish.
     * This guards against the previous implementation that relied on
     * {@code ordinal()} and would silently re-number constants when the enum order changed.
     */
    @Test
    public void typeNumber_isConsistentWithProtoEnum()
    {
        assertEquals(MetadataProto.File.Type.TEMPORARY_INGEST.getNumber(),
                File.Type.TEMPORARY_INGEST.getNumber());
        assertEquals(MetadataProto.File.Type.REGULAR.getNumber(),
                File.Type.REGULAR.getNumber());
        assertEquals(MetadataProto.File.Type.TEMPORARY_GC.getNumber(),
                File.Type.TEMPORARY_GC.getNumber());
        assertEquals(MetadataProto.File.Type.RETIRED.getNumber(),
                File.Type.RETIRED.getNumber());
    }

    // -------------------------------------------------------------------------
    // File.Type.valueOf(int) — happy path + boundaries
    // -------------------------------------------------------------------------

    @Test
    public void typeValueOf_resolvesAllKnownNumbers()
    {
        assertSame(File.Type.TEMPORARY_INGEST, File.Type.valueOf(0));
        assertSame(File.Type.REGULAR, File.Type.valueOf(1));
        assertSame(File.Type.TEMPORARY_GC, File.Type.valueOf(2));
        assertSame(File.Type.RETIRED, File.Type.valueOf(3));
    }

    @Test
    public void typeValueOf_rejectsInvalidNumbers()
    {
        // Test various boundary cases for invalid type numbers
        int[] invalidNumbers = {-1, 4, Integer.MAX_VALUE, Integer.MIN_VALUE};
        
        for (int invalidNumber : invalidNumbers)
        {
            try
            {
                File.Type.valueOf(invalidNumber);
                fail("expected InvalidArgumentException for number: " + invalidNumber);
            }
            catch (InvalidArgumentException expected)
            {
                assertNotNull("Exception message should not be null for number: " + invalidNumber, 
                        expected.getMessage());
            }
        }
    }

    /**
     * Round-trip: every constant survives {@code num -> valueOf -> getNumber}.
     */
    @Test
    public void typeValueOf_roundTripForAllConstants()
    {
        for (File.Type t : File.Type.values())
        {
            assertSame("round-trip failed for " + t,
                    t, File.Type.valueOf(t.getNumber()));
        }
    }

    // -------------------------------------------------------------------------
    // cleanupAt — getter / setter
    // -------------------------------------------------------------------------

    @Test
    public void cleanupAt_defaultsToNullOnNoArgConstructor()
    {
        File f = new File();
        assertNull("a freshly constructed File must have a null cleanupAt", f.getCleanupAt());
    }

    @Test
    public void cleanupAt_setterAcceptsValueAndNull()
    {
        File f = new File();
        f.setCleanupAt(123_456_789L);
        assertEquals(Long.valueOf(123_456_789L), f.getCleanupAt());

        // explicit clear must be supported (used after promote-to-REGULAR)
        f.setCleanupAt(null);
        assertNull(f.getCleanupAt());
    }

    // -------------------------------------------------------------------------
    // toProto / fromProto round-trip
    // -------------------------------------------------------------------------

    /**
     * When {@code cleanupAt == null}, {@link File#toProto()} must NOT set the optional
     * field on the wire.  Otherwise downstream consumers calling {@code hasCleanupAt()}
     * would see a spurious zero deadline.
     */
    @Test
    public void toProto_omitsCleanupAt_whenDomainValueIsNull()
    {
        File f = makeFile(1L, "n.pxl", File.Type.TEMPORARY_INGEST, 1, 0L, 0L, 1L, null);

        MetadataProto.File proto = f.toProto();

        assertFalse("cleanupAt must be absent on the wire when domain value is null",
                proto.hasCleanupAt());
    }

    /**
     * cleanupAt = 0L is a legitimate value (epoch start); it must NOT be confused with "absent".
     * Without this guard, a naïve {@code if (cleanupAt != 0)} check would silently drop the field.
     */
    @Test
    public void toProto_includesCleanupAt_whenValueIsZero()
    {
        File f = makeFile(1L, "z.pxl", File.Type.RETIRED, 1, 0L, 0L, 1L, 0L);

        MetadataProto.File proto = f.toProto();

        assertTrue("cleanupAt = 0L must be carried on the wire (zero != absent)",
                proto.hasCleanupAt());
        assertEquals(0L, proto.getCleanupAt());
    }

    @Test
    public void fromProto_preservesCleanupAt_whenSet()
    {
        long deadline = 1_700_000_123_456L;
        MetadataProto.File proto = MetadataProto.File.newBuilder()
                .setId(42L)
                .setName("retired.pxl")
                .setTypeValue(File.Type.RETIRED.getNumber())
                .setNumRowGroup(2)
                .setMinRowId(0L)
                .setMaxRowId(127L)
                .setPathId(9L)
                .setCleanupAt(deadline)
                .build();

        File f = new File(proto);

        assertEquals(42L, f.getId());
        assertEquals("retired.pxl", f.getName());
        assertSame(File.Type.RETIRED, f.getType());
        assertEquals(2, f.getNumRowGroup());
        assertEquals(0L, f.getMinRowId());
        assertEquals(127L, f.getMaxRowId());
        assertEquals(9L, f.getPathId());
        assertNotNull("cleanupAt must be retained from the proto", f.getCleanupAt());
        assertEquals(Long.valueOf(deadline), f.getCleanupAt());
    }

    /**
     * If the proto omits the optional cleanupAt, the domain object MUST observe {@code null}
     * (not 0L).  This is the reciprocal of {@link #toProto_omitsCleanupAt_whenDomainValueIsNull()}.
     */
    @Test
    public void fromProto_returnsNullCleanupAt_whenAbsent()
    {
        MetadataProto.File proto = MetadataProto.File.newBuilder()
                .setId(1L)
                .setName("tmp.pxl")
                .setTypeValue(File.Type.TEMPORARY_GC.getNumber())
                .setNumRowGroup(1)
                .setMinRowId(0L)
                .setMaxRowId(0L)
                .setPathId(1L)
                .build();

        File f = new File(proto);

        assertNull("absent cleanupAt on the wire must materialise as null in the domain",
                f.getCleanupAt());
    }

    /**
     * End-to-end round-trip — domain → proto → domain — must be lossless for every {@link File.Type}.
     */
    @Test
    public void roundTrip_domainProtoDomain_isLossless_forEveryType()
    {
        for (File.Type t : File.Type.values())
        {
            // RETIRED carries cleanupAt; the others should not.  We deliberately set cleanupAt
            // independently of type to verify the domain object preserves whatever it is given.
            Long cleanup = (t == File.Type.RETIRED) ? 1_700_000_000_999L : null;
            File original = makeFile(7L, "x_" + t + ".pxl", t, 1, 0L, 63L, 3L, cleanup);

            File restored = new File(original.toProto());

            assertEquals("id mismatch for " + t, original.getId(), restored.getId());
            assertEquals("name mismatch for " + t, original.getName(), restored.getName());
            assertSame("type mismatch for " + t, original.getType(), restored.getType());
            assertEquals("numRowGroup mismatch for " + t,
                    original.getNumRowGroup(), restored.getNumRowGroup());
            assertEquals("minRowId mismatch for " + t,
                    original.getMinRowId(), restored.getMinRowId());
            assertEquals("maxRowId mismatch for " + t,
                    original.getMaxRowId(), restored.getMaxRowId());
            assertEquals("pathId mismatch for " + t,
                    original.getPathId(), restored.getPathId());
            assertEquals("cleanupAt mismatch for " + t,
                    original.getCleanupAt(), restored.getCleanupAt());
        }
    }

    // -------------------------------------------------------------------------
    // convertFiles / revertFiles
    // -------------------------------------------------------------------------

    @Test
    public void convertFiles_handlesEmptyList()
    {
        List<File> result = File.convertFiles(Collections.emptyList());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void convertFiles_rejectsNullInput()
    {
        File.convertFiles(null);
    }

    @Test
    public void convertFiles_thenRevertFiles_isLossless()
    {
        MetadataProto.File p1 = MetadataProto.File.newBuilder()
                .setId(10L).setName("a.pxl")
                .setTypeValue(File.Type.REGULAR.getNumber())
                .setNumRowGroup(1).setMinRowId(0L).setMaxRowId(63L).setPathId(1L)
                .build();
        MetadataProto.File p2 = MetadataProto.File.newBuilder()
                .setId(11L).setName("b.pxl")
                .setTypeValue(File.Type.RETIRED.getNumber())
                .setNumRowGroup(2).setMinRowId(64L).setMaxRowId(127L).setPathId(1L)
                .setCleanupAt(1_700_000_000_000L)
                .build();

        List<File> domain = File.convertFiles(Arrays.asList(p1, p2));
        assertEquals(2, domain.size());
        assertSame(File.Type.REGULAR, domain.get(0).getType());
        assertNull(domain.get(0).getCleanupAt());
        assertSame(File.Type.RETIRED, domain.get(1).getType());
        assertEquals(Long.valueOf(1_700_000_000_000L), domain.get(1).getCleanupAt());

        List<MetadataProto.File> back = File.revertFiles(domain);
        assertEquals(2, back.size());
        assertEquals(p1, back.get(0));
        assertEquals(p2, back.get(1));
    }

    @Test(expected = NullPointerException.class)
    public void revertFiles_rejectsNullInput()
    {
        File.revertFiles(null);
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private static File makeFile(long id, String name, File.Type type,
                                 int numRowGroup, long minRowId, long maxRowId,
                                 long pathId, Long cleanupAt)
    {
        File f = new File();
        f.setId(id);
        f.setName(name);
        f.setType(type);
        f.setNumRowGroup(numRowGroup);
        f.setMinRowId(minRowId);
        f.setMaxRowId(maxRowId);
        f.setPathId(pathId);
        f.setCleanupAt(cleanupAt);
        return f;
    }
}
