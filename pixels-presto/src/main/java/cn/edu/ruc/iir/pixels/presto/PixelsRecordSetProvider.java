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
import cn.edu.ruc.iir.pixels.presto.impl.PixelsMetadataReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto
 * @ClassName: PixelsRecordSetProvider
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-20 22:21
 **/
public class PixelsRecordSetProvider
        implements ConnectorRecordSetProvider {
    private final Logger log = Logger.get(PixelsRecordSetProvider.class.getName());
    private final String connectorId;
    private final PixelsMetadataReader pixelsMetadataReader;
    private final FSFactory fsFactory;

    @Inject
    public PixelsRecordSetProvider(PixelsConnectorId connectorId, PixelsMetadataReader pixelsMetadataReader, FSFactory fsFactory) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pixelsMetadataReader = requireNonNull(pixelsMetadataReader, "pixelsMetadataReader is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
        log.info("connectorId: " + connectorId.toString());
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        log.info("PixelsRecordSetProvider getRecordSet: " + connectorId.toString());
        requireNonNull(split, "split is null");
        PixelsSplit pixelsSplit = (PixelsSplit) split;
        checkArgument(pixelsSplit.getConnectorId().equals(connectorId), "connectorId is not for this connector");

        PixelsTable pixelsTable = pixelsMetadataReader.getTable(connectorId, pixelsSplit.getSchemaName(), pixelsSplit.getTableName());

        ImmutableList.Builder<PixelsColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((PixelsColumnHandle) handle);
        }
        log.info("new PixelsRecordSet: " + pixelsSplit.getSchemaName() + ", " + pixelsSplit.getTableName() + ", " + pixelsSplit.getPath());
        return new PixelsRecordSet(pixelsSplit, handles.build(), pixelsTable, fsFactory);
    }
}
