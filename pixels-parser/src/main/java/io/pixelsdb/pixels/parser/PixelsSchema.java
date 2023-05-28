package io.pixelsdb.pixels.parser;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto Pixels internal schema representation.
 */
public class PixelsSchema extends AbstractSchema
{
    private final String name;
    private final MetadataService metadataService;

    PixelsSchema(String name, MetadataService metadataService)
    {
        this.name = name;
        this.metadataService = metadataService;
    }

    @Override
    protected Map<String, Table> getTableMap()
    {
        final Map<String, Table> tables = new HashMap<>();
        try
        {
            List<io.pixelsdb.pixels.common.metadata.domain.Table> pixelsTables = this.metadataService.getTables(this.name);
            for (io.pixelsdb.pixels.common.metadata.domain.Table t : pixelsTables)
            {
                String tableName = t.getName();
                tables.put(tableName, new PixelsTable(this.name, tableName, this.metadataService));
            }
            return tables;
        } catch (MetadataException e)
        {
            throw new RuntimeException(e);
        }
    }
}
