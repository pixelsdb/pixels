package io.pixelsdb.pixels.parser;

import io.pixelsdb.pixels.common.metadata.MetadataService;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Factory that creates a PixelsSchema.
 */
public class PixelsSchemaFactory implements SchemaFactory
{
    private static MetadataService metadataService;

    public PixelsSchemaFactory(MetadataService metadataService)
    {
        this.metadataService = metadataService;
    }

    @Override
    public Schema create(
            SchemaPlus parentSchema, String name, Map<String, Object> operand)
    {
        return new PixelsSchema(name, this.metadataService);
    }
}
