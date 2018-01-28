/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.*;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto
 * @ClassName: PixelsRecordCursor
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-20 22:21
 **/
public class PixelsRecordCursor
        implements RecordCursor {
    private static Logger logger = Logger.get(PixelsRecordCursor.class);
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    // TODO This should be a config option as it may be different for different log files
    public static final DateTimeFormatter ISO_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISODateTimeFormat.dateHourMinuteSecondFraction())
            .appendTimeZoneOffset("Z", true, 2, 2)
            .toFormatter();

    private final List<PixelsColumnHandle> columns;
    private final int[] fieldToColumnIndex;
    private final HostAddress address;
    private final FilesReader reader;
    private List<String> fields;
    private static FSFactory fsFactory;

    public PixelsRecordCursor(PixelsTable pixelsTable, List<PixelsColumnHandle> columns, SchemaTableName tableName, HostAddress address, TupleDomain<PixelsColumnHandle> predicate, FSFactory fsFactory) {
        this.columns = requireNonNull(columns, "columns is null");
        this.address = requireNonNull(address, "address is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");

        fieldToColumnIndex = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            PixelsColumnHandle columnHandle = columns.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
            logger.info("columns" + columns.get(i).getColumnName());
        }
        logger.info("getFilesReader begin");
        this.reader = getFilesReader(pixelsTable, tableName, predicate);
        logger.info("PixelsRecordCursor Constructor");
    }

    private static FilesReader getFilesReader(PixelsTable pixelsTable, SchemaTableName tableName, TupleDomain<PixelsColumnHandle> predicate) {
        PixelsTableHandle table = pixelsTable.getTableHandle();
        String path = table.getPath();
        logger.info("PixelsRecordCursor path: " + path);
        List<Path> fileNames = fsFactory.listFiles(path);
        try {
            return new FilesReader(OptionalInt.empty(), fileNames.iterator(), predicate);
        } catch (IOException e) {
            logger.info("FilesReader error");
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columns.size(), "Invalid field index");
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        try {
            fields = reader.readFields();
            return fields != null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        if (columnIndex == -1) {
            return address.toString();
        }
        if (columnIndex >= fields.size()) {
            return null;
        }
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field) {
        if (getType(field).equals(TIMESTAMP)) {
            return ISO_FORMATTER.parseDateTime(getFieldValue(field)).getMillis();
        } else {
            checkFieldType(field, BIGINT, INTEGER);
            return Long.parseLong(getFieldValue(field));
        }
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columns.size(), "Invalid field index");
        String fieldValue = getFieldValue(field);
        return "null".equals(fieldValue) || Strings.isNullOrEmpty(fieldValue);
    }

    private void checkFieldType(int field, Type... expected) {
        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) {
                return;
            }
        }
        String expectedTypes = Joiner.on(", ").join(expected);
        throw new IllegalArgumentException(format("Expected field %s to be type %s but is %s", field, expectedTypes, actual));
    }

    @Override
    public void close() {
        reader.close();
    }

    private static class FilesReader {
        private static Logger logger = Logger.get(FilesReader.class);
        private final Iterator<Path> files;
        private final Optional<Domain> domain;
        private final OptionalInt timestampOrdinalPosition;

        private BufferedReader reader;

        public FilesReader(OptionalInt timestampOrdinalPosition, Iterator<Path> files, TupleDomain<PixelsColumnHandle> predicate)
                throws IOException {
            requireNonNull(files, "files is null");
            this.files = files;

            requireNonNull(predicate, "predicate is null");
            this.domain = getDomain(timestampOrdinalPosition, predicate);

            this.timestampOrdinalPosition = timestampOrdinalPosition;

            reader = createNextReader();
        }

        private static Optional<Domain> getDomain(OptionalInt timestampOrdinalPosition, TupleDomain<PixelsColumnHandle> predicate) {
            Optional<Map<PixelsColumnHandle, Domain>> domains = predicate.getDomains();
            Domain domain = null;
            if (domains.isPresent() && timestampOrdinalPosition.isPresent()) {
                Map<PixelsColumnHandle, Domain> domainMap = domains.get();
                Set<Domain> timestampDomain = domainMap.entrySet().stream()
                        .filter(entry -> entry.getKey().getOrdinalPosition() == timestampOrdinalPosition.getAsInt())
                        .map(Map.Entry::getValue)
                        .collect(toSet());

                if (!timestampDomain.isEmpty()) {
                    domain = Iterables.getOnlyElement(timestampDomain);
                }
            }
            return Optional.ofNullable(domain);
        }

        private BufferedReader createNextReader()
                throws IOException {
            if (!files.hasNext()) {
                return null;
            }
            FileSystem fileSystem = fsFactory.getFileSystem().orElse(null);
            Path file = files.next();
            logger.info("createNextReader " + file.getName());
            FSDataInputStream fsr = fileSystem.open(file);

            return new BufferedReader(new InputStreamReader(fsr));
        }


        public List<String> readFields()
                throws IOException {
            List<String> fields = null;
            boolean newReader = false;

            while (fields == null) {
                if (reader == null) {
                    return null;
                }
                String line = reader.readLine();
                if (line != null) {
                    fields = LINE_SPLITTER.splitToList(line);
                    if (!newReader || meetsPredicate(fields)) {
                        return fields;
                    }
                }
                reader.close();
                reader = createNextReader();
                newReader = true;
            }
            return fields;
        }

        private boolean meetsPredicate(List<String> fields) {
            if (!timestampOrdinalPosition.isPresent() || !domain.isPresent()) {
                return true;
            }

            long millis = ISO_FORMATTER.parseDateTime(fields.get(timestampOrdinalPosition.getAsInt())).getMillis();
            return domain.get().includesNullableValue(millis);
        }

        public void close() {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
