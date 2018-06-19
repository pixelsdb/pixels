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

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Splits;
import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsMetadataReader;
import cn.edu.ruc.iir.pixels.presto.split.builder.PatternBuilder;
import cn.edu.ruc.iir.pixels.presto.split.domain.AccessPattern;
import cn.edu.ruc.iir.pixels.presto.split.domain.ColumnSet;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexEntry;
import cn.edu.ruc.iir.pixels.presto.split.index.IndexFactory;
import cn.edu.ruc.iir.pixels.presto.split.index.Inverted;
import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_INVERTED_INDEX_ERROR;
import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto
 * @ClassName: PixelsSplitManager
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-20 19:16
 **/
public class PixelsSplitManager
        implements ConnectorSplitManager {
    private final Logger log = Logger.get(PixelsSplitManager.class);
    private final String connectorId;
    private final FSFactory fsFactory;
    private final PixelsMetadataReader metadataReader;

    @Inject
    public PixelsSplitManager(PixelsConnectorId connectorId, PixelsMetadataReader metadataReader, FSFactory fsFactory) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
        this.metadataReader = requireNonNull(metadataReader, "metadataReader is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy) {
        PixelsTableLayoutHandle layoutHandle = (PixelsTableLayoutHandle) layout;
        PixelsTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<PixelsColumnHandle> constraint = layoutHandle.getConstraint()
                .transform(PixelsColumnHandle.class::cast);
        Set<PixelsColumnHandle> desiredColumns = layoutHandle.getDesiredColumns().stream().map(PixelsColumnHandle.class::cast)
                .collect(toSet());

        String schemaName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();
        List<Layout> layouts;
        try {
            layouts = metadataReader.getDataLayouts(tableHandle.getSchemaName(),
                    tableHandle.getTableName());
        } catch (MetadataException e) {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }

        Layout curLayout = layouts.get(0);
        int version = curLayout.getVersion();

        // index is exist
        IndexEntry indexEntry = new IndexEntry(schemaName, tableName);
        Inverted index = (Inverted) IndexFactory.Instance().getIndex(indexEntry);
        if (index == null) {
            log.info("action null");
            index = getInverted(curLayout, indexEntry);
        } else {
            log.info("action not null");
            int indexVersion = index.getVersion();
            // todo update
            if (indexVersion < version) {
                log.info("action not null update");
                index = getInverted(curLayout, indexEntry);
            }
        }
        //todo get split size
        ColumnSet columnSet = new ColumnSet();
        for (PixelsColumnHandle column : desiredColumns) {
            log.info(column.getColumnName());
            columnSet.addColumn(column.getColumnName());
        }

        AccessPattern bestPattern = index.search(columnSet);
        log.info("bestPattern: " + bestPattern.toString());
        int splitSize = bestPattern.getSplitSize();

        List<Path> files = new ArrayList<>();
        for (Layout l : layouts) {
            files.addAll(fsFactory.listFiles(l.getOrderPath()));
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        files.forEach(file -> splits.add(new PixelsSplit(connectorId,
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                file.toString(), 0, -1,
                fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE), constraint)));

        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }

    private Inverted getInverted(Layout curLayout, IndexEntry indexEntry) {
        String columnInfo = curLayout.getOrder();
        String splitInfo = curLayout.getSplits();
        Splits split = JSON.parseObject(splitInfo, Splits.class);
        Order order = JSON.parseObject(columnInfo, Order.class);
        List<String> columnOrder = order.getColumnOrder();
        Inverted index;
        try {
            index = new Inverted(columnOrder, PatternBuilder.build(columnOrder, split));
            IndexFactory.Instance().cacheIndex(indexEntry, index);
        } catch (IOException e) {
            log.info("getInverted error: " + e.getMessage());
            throw new PrestoException(PIXELS_INVERTED_INDEX_ERROR, e);
        }
        return index;
    }
}