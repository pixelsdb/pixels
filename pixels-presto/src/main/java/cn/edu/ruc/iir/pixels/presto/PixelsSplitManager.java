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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
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
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle tableLayout,
                                          SplitSchedulingStrategy splitSchedulingStrategy)
    {
        PixelsTableLayoutHandle layoutHandle = (PixelsTableLayoutHandle) tableLayout;
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
        }
        catch (MetadataException e) {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }

        List<ConnectorSplit> pixelsSplits = new ArrayList<>();
        for (Layout layout : layouts)
        {
            // get index
            int version = layout.getVersion();
            IndexEntry indexEntry = new IndexEntry(schemaName, tableName);
            Inverted index = (Inverted) IndexFactory.Instance().getIndex(indexEntry);
            Order order = JSON.parseObject(layout.getOrder(), Order.class);
            Splits splits = JSON.parseObject(layout.getSplits(), Splits.class);
            if (index == null)
            {
                log.debug("action null");
                index = getInverted(order, splits, indexEntry);
            }
            else
            {
                log.debug("action not null");
                int indexVersion = index.getVersion();
                // todo update
                if (indexVersion < version) {
                    log.debug("action not null update");
                    index = getInverted(order, splits, indexEntry);
                }
            }
            // get split size
            ColumnSet columnSet = new ColumnSet();
            for (PixelsColumnHandle column : desiredColumns) {
                log.debug(column.getColumnName());
                columnSet.addColumn(column.getColumnName());
            }
            AccessPattern bestPattern = index.search(columnSet);
            log.debug("bestPattern: " + bestPattern.toString());
            int splitSize = bestPattern.getSplitSize();
            int rowGroupNum = splits.getNumRowGroupInBlock();
            // add splits in orderPath
            fsFactory.listFiles(layout.getOrderPath()).forEach(file -> pixelsSplits.add(
                    new PixelsSplit(connectorId,
                            tableHandle.getSchemaName(), tableHandle.getTableName(),
                            file.toString(), 0, -1,
                            fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE), constraint)));
            // add splits in compactionPath
            int curFileRGIdx;
            for (Path file : fsFactory.listFiles(layout.getCompactPath()))
            {
                curFileRGIdx = 0;
                while (curFileRGIdx < rowGroupNum)
                {
                    PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                            tableHandle.getSchemaName(), tableHandle.getTableName(),
                            file.toString(), curFileRGIdx, splitSize,
                            fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE), constraint);
                    pixelsSplits.add(pixelsSplit);
                    curFileRGIdx += splitSize;
                }
            }
        }

        Collections.shuffle(pixelsSplits);

        return new FixedSplitSource(pixelsSplits);
    }

    private Inverted getInverted(Order order, Splits splits, IndexEntry indexEntry) {
        List<String> columnOrder = order.getColumnOrder();
        Inverted index;
        try {
            index = new Inverted(columnOrder, PatternBuilder.build(columnOrder, splits));
            IndexFactory.Instance().cacheIndex(indexEntry, index);
        } catch (IOException e) {
            log.info("getInverted error: " + e.getMessage());
            throw new PrestoException(PIXELS_INVERTED_INDEX_ERROR, e);
        }
        return index;
    }
}