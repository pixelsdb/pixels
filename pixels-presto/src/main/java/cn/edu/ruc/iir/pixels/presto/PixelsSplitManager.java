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

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.presto.client.MetadataService;
import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsMetadataReader;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

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
    private final PixelsMetadataReader pixelsMetadataReader;
    private final FSFactory fsFactory;

    @Inject
    public PixelsSplitManager(PixelsConnectorId connectorId, PixelsMetadataReader pixelsMetadataReader, FSFactory fsFactory) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pixelsMetadataReader = requireNonNull(pixelsMetadataReader, "pixelsMetadataReader is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy) {
        PixelsTableLayoutHandle layoutHandle = (PixelsTableLayoutHandle) layout;
        PixelsTableHandle tableHandle = layoutHandle.getTable();
        PixelsTable table = pixelsMetadataReader.getTable(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        String tablePath = tableHandle.getPath();
        List<ConnectorSplit> splits = new ArrayList<>();

        log.info("PixelsColumnHandle layoutHandle: " + layoutHandle.toString());
        TupleDomain<PixelsColumnHandle> constraint = layoutHandle.getConstraint()
                .transform(PixelsColumnHandle.class::cast);
        log.info("PixelsColumnHandle constraint: " + constraint.toString());
        // push down
        Map<PixelsColumnHandle, Domain> domains = constraint.getDomains().get();
        log.info("domains size: " + domains.size());
        List<PixelsColumnHandle> indexedColumns = new ArrayList<>();
        // compose partitionId by using indexed column
        for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet()) {
            PixelsColumnHandle column = (PixelsColumnHandle) entry.getKey();
            log.info("column: " + column.getColumnName() + " " + column.getColumnType());
            Domain domain = entry.getValue();
            if (domain.isSingleValue()) {
                indexedColumns.add(column);
                // Only one indexed column predicate can be pushed down.
            }
            log.info("domain: " + domain.isSingleValue());
        }
        log.info("indexedColumns: " + indexedColumns.toString());

        List<Layout> catalogList = MetadataService.getLayoutsByTblName(tableHandle.getTableName());
        List<Path> files = new ArrayList<>();
        for (Layout l : catalogList) {
            files.addAll(fsFactory.listFiles(l.getLayInitPath()));
            log.info("Path: " + l.getLayInitPath());
        }
        log.info("Path: " + tablePath);

        files.forEach(file -> splits.add(new PixelsSplit(connectorId,
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                file.toString(), 0, -1,
                fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE), constraint)));

        Collections.shuffle(splits);

        log.info("files forEach: " + files.size());
        return new FixedSplitSource(splits);
    }
}