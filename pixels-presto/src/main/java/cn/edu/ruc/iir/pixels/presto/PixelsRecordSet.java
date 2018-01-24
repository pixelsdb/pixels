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
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto
 * @ClassName: PixelsRecordSet
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-20 22:21
 **/
public class PixelsRecordSet
        implements RecordSet {
    private final Logger log = Logger.get(PixelsRecordSet.class.getName());
    private final List<PixelsColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final HostAddress address;
    private final SchemaTableName tableName;
    private final PixelsTable pixelsTable;
    private final FSFactory fsFactory;

    public PixelsRecordSet(PixelsSplit split, List<PixelsColumnHandle> columnHandles, PixelsTable pixelsTable, FSFactory fsFactory) {
        requireNonNull(split, "split is null");

        this.effectivePredicate = split.getConstraint();
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (PixelsColumnHandle column : columnHandles) {

            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        List<HostAddress> addresses;
        if (split.getAddresses().size() > 1) {
            addresses = new ArrayList<HostAddress>();
            addresses.add(split.getAddresses().get(0));
        } else {
            addresses = split.getAddresses();
        }

        this.address = Iterables.getOnlyElement(addresses);
        this.tableName = split.toSchemaTableName();
        this.pixelsTable = requireNonNull(pixelsTable, "pixelsTable is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
        log.info("PixelsRecordSet Constructor");
    }

    private final TupleDomain<PixelsColumnHandle> effectivePredicate;

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        log.info("new PixelsRecordCursor");
        return new PixelsRecordCursor(pixelsTable, columnHandles, tableName, address, effectivePredicate, fsFactory);
    }
}
