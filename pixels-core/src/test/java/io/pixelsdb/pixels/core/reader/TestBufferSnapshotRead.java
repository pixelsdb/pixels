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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.reader;

import static org.junit.Assert.*;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.retina.RetinaProto;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** No external services: real columnar serialization, local object reads, and snapshot bitmaps. */
public class TestBufferSnapshotRead {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final TypeDescription schema = TypeDescription.fromString("struct<v:bigint>");
    private final Map<String, String> original = new HashMap<>();
    private Path root;
    private Storage storage;

    @Before
    public void setup() throws Exception {
        root = temporary.newFolder().toPath();
        ConfigFactory config = ConfigFactory.Instance();
        Map<String, String> settings = new HashMap<>();
        settings.put("retina.enable", "true");
        settings.put("enabled.storage.schemes", "file");
        settings.put("retina.reader.prefetch.threads", "2");
        settings.put("retina.buffer.object.storage.folder", root.toUri().toString());
        settings.forEach(
                (key, value) -> {
                    original.put(key, config.getProperty(key));
                    config.addProperty(key, value);
                });
        storage = StorageFactory.Instance().getStorage("file");
    }

    @After
    public void restore() {
        original.forEach(
                (key, value) -> {
                    if (value != null) ConfigFactory.Instance().addProperty(key, value);
                });
    }

    private byte[] batch(long timestamp, long... values) {
        VectorizedRowBatch batch =
                schema.createRowBatchWithHiddenColumn(64, TypeDescription.VectorLayout.NONE);
        for (long value : values) {
            ((LongColumnVector) batch.cols[0]).add(value);
            ((LongColumnVector) batch.cols[batch.cols.length - 1]).add(timestamp);
            batch.size++;
        }
        return batch.serialize();
    }

    private void object(long id, byte[] bytes) throws IOException {
        Path folder = root.resolve("9/0");
        Files.createDirectories(folder);
        Files.write(folder.resolve("node_" + id), bytes);
    }

    private static RetinaProto.VisibilityBitmap bitmap(long bits) {
        return RetinaProto.VisibilityBitmap.newBuilder().addBitmap(bits).build();
    }

    private PixelsRecordReaderBufferImpl reader(
            byte[] active, List<Long> ids, List<RetinaProto.VisibilityBitmap> masks, long timestamp)
            throws IOException {
        PixelsReaderOption options = new PixelsReaderOption();
        options.includeCols(new String[] {"v"});
        options.transTimestamp(timestamp);
        return new PixelsRecordReaderBufferImpl(
                options, "node", active, ids, masks, storage, 9, 0, schema);
    }

    private List<Long> values(PixelsRecordReaderBufferImpl reader) throws IOException {
        List<Long> values = new ArrayList<>();
        while (!reader.isEndOfFile()) {
            VectorizedRowBatch batch = reader.readBatch();
            if (batch.size > 0)
                assertFalse("A nonempty final batch must not be discarded as EOF", batch.endOfFile);
            for (int i = 0; i < batch.size; i++)
                values.add(((LongColumnVector) batch.cols[0]).vector[i]);
        }
        return values;
    }

    @Test
    public void associatesVisibilityWithTheReturnedSegment() throws Exception {
        object(1, batch(10, 3, 4));
        object(2, batch(20, 5, 6));
        try (PixelsRecordReaderBufferImpl reader =
                reader(
                        batch(10, 1, 2),
                        Arrays.asList(1L, 2L),
                        Arrays.asList(bitmap(2), bitmap(1), bitmap(0)),
                        10)) {
            assertTrue(reader.isValid());
            assertEquals(Arrays.asList(1L, 4L), values(reader));
        }
    }

    @Test
    public void drainsMoreSegmentsThanThePrefetchWindow() throws Exception {
        List<Long> ids = new ArrayList<>(), expected = new ArrayList<>();
        List<RetinaProto.VisibilityBitmap> masks = new ArrayList<>();
        masks.add(bitmap(0));
        for (int i = 0; i < 40; i++) {
            ids.add((long) i);
            object(i, batch(10, 2L * i, 2L * i + 1));
            masks.add(bitmap(0));
            expected.add(2L * i);
            expected.add(2L * i + 1);
        }
        try (PixelsRecordReaderBufferImpl reader = reader(new byte[0], ids, masks, 10)) {
            assertEquals(expected, values(reader));
            assertEquals(40, reader.getNumReadRequests());
        }
    }

    @Test
    public void propagatesCorruptObjectFailure() throws Exception {
        object(1, new byte[] {0, 1, 2});
        try (PixelsRecordReaderBufferImpl reader =
                reader(
                        new byte[0],
                        Collections.singletonList(1L),
                        Arrays.asList(bitmap(0), bitmap(0)),
                        10)) {
            assertThrows(IOException.class, reader::readBatch);
        }
    }

    @Test
    public void acceptsAnEmptyUnmaterializedTable() throws Exception {
        try (PixelsRecordReaderBufferImpl reader =
                reader(new byte[0], Collections.emptyList(), Collections.emptyList(), 10)) {
            assertEquals(Collections.emptyList(), values(reader));
        }
    }
}
