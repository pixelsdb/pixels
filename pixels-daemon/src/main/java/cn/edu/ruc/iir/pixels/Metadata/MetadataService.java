package cn.edu.ruc.iir.pixels.Metadata;

import com.facebook.presto.spi.ColumnMetadata;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.Metadata
 * @ClassName: MetadataService
 * @Description: get metaData
 * @author: tao
 * @date: Create in 2018-01-21 21:54
 **/
public class MetadataService {


    public static List<ColumnMetadata> getColumnMetadata() {
        List<ColumnMetadata> list = new ArrayList<ColumnMetadata>();
        ColumnMetadata c1 = new ColumnMetadata("id", createUnboundedVarcharType(), "", false);
        list.add(c1);
        ColumnMetadata c2 = new ColumnMetadata("x", createUnboundedVarcharType(), "", false);
        list.add(c2);
        ColumnMetadata c3 = new ColumnMetadata("y", createUnboundedVarcharType(), "", false);
        list.add(c3);
        return list;
    }


}
