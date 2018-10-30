package cn.edu.ruc.iir.pixels.load.pc;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Config {
    private String originalDataPath;
    private String dbName;
    private String tableName;
    private int maxRowNum;
    private String regex;

    private String format;

    private String pixelsPath;
    private String schema;
    private int[] orderMapping;

    protected Config() {
    }

    public String getPixelsPath() {
        return pixelsPath;
    }

    public String getSchema() {
        return schema;
    }

    public int[] getOrderMapping() {
        return orderMapping;
    }

    public int getMaxRowNum() {
        return maxRowNum;
    }

    public String getRegex() {
        return regex;
    }

    public String getFormat() {
        return format;
    }

    Config(String originalDataPath, String dbName, String tableName, int maxRowNum, String regex, String format) {
        this.originalDataPath = originalDataPath;
        this.dbName = dbName;
        this.tableName = tableName;
        this.maxRowNum = maxRowNum;
        this.regex = regex;
        this.format = format;
    }

    boolean load(ConfigFactory configFactory) throws IOException, MetadataException {
        // init metadata service
        String metaHost = configFactory.getProperty("metadata.server.host");
        int metaPort = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        MetadataService metadataService = new MetadataService(metaHost, metaPort);
        // get columns of the specified table
        List<Column> columns = metadataService.getColumns(dbName, tableName);
        int colSize = columns.size();
        // record original column names and types
        String[] originalColNames = new String[colSize];
        String[] originalColTypes = new String[colSize];
        for (int i = 0; i < colSize; i++) {
            originalColNames[i] = columns.get(i).getName();
            originalColTypes[i] = columns.get(i).getType();
        }
        // get the latest layout for writing
        List<Layout> layouts = metadataService.getLayouts(dbName, tableName);
        Layout writingLayout = null;
        int writingLayoutVersion = -1;
        for (Layout layout : layouts) {
            if (layout.getPermission() > 0) {
                if (layout.getVersion() > writingLayoutVersion) {
                    writingLayout = layout;
                    writingLayoutVersion = layout.getVersion();
                }
            }
        }
        // no layouts for writing currently
        if (writingLayout == null) {
            return false;
        }
        // get the column order of the latest writing layout
        Order order = JSON.parseObject(writingLayout.getOrder(), Order.class);
        List<String> layoutColumnOrder = order.getColumnOrder();
        // get path of loading
        String loadingDataPath = writingLayout.getOrderPath();
        if (!loadingDataPath.endsWith("/")) {
            loadingDataPath += "/";
        }
        // check size consistency
        if (layoutColumnOrder.size() != colSize) {
            return false;
        }
        // map the column order of the latest writing layout to the original column order
        int[] orderMapping = new int[colSize];
        List<String> originalColNameList = Arrays.asList(originalColNames);
        for (int i = 0; i < colSize; i++) {
            int index = originalColNameList.indexOf(layoutColumnOrder.get(i));
            if (index >= 0) {
                orderMapping[i] = index;
            } else {
                return false;
            }
        }
        // construct pixels schema based on the column order of the latest writing layout
        StringBuilder schemaBuilder = new StringBuilder("struct<");
        for (int i = 0; i < colSize; i++) {
            String name = layoutColumnOrder.get(i);
            String type = originalColTypes[orderMapping[i]];
            if (type.equals("integer")) {
                type = "int";
            } else if (type.equals("long")) {
                type = "bigint";
            } else if (type.equals("varchar")) {
                type = "string";
            }
            schemaBuilder.append(name).append(":").append(type)
                    .append(",");
        }
        schemaBuilder.replace(schemaBuilder.length() - 1, schemaBuilder.length(), ">");

        // init the params
        this.pixelsPath = loadingDataPath;
        this.schema = schemaBuilder.toString();
        this.orderMapping = orderMapping;
        return true;
    }

}
