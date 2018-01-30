package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Schema;
import org.junit.Test;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.client
 * @ClassName: TestMetadataService
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-30 15:36
 **/
public class TestMetadataService {

    @Test
    public void testGetSchemaNames() {
        List<Schema> schemas = MetadataService.getSchemas();
        System.out.println(schemas.size());
    }
}
