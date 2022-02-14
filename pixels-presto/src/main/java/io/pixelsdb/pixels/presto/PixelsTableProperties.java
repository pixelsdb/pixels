package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Class contains all table properties for the Pixels connector. Used when creating a table:
 * <p>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (storage = 'hdfs');
 * </p>
 *
 * Created at: 14/02/2022
 * Author: hank
 */
public class PixelsTableProperties
{
    public static final String STORAGE = "storage";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public PixelsTableProperties()
    {
        PropertyMetadata<String> s1 = new PropertyMetadata<>(
                STORAGE,
                "The storage scheme of the table.",
                VARCHAR,
                String.class,
                "hdfs",
                false,
            String.class::cast,
            object -> object);

        tableProperties = ImmutableList.of(s1);
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
