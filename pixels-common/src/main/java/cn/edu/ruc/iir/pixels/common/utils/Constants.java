package cn.edu.ruc.iir.pixels.common.utils;

/**
 * pixels
 *
 * @author guodong
 */
public final class Constants
{
    public static int VERSION = 1;
    public static String MAGIC = "PIXELS";

    public static int DEFAULT_HDFS_BLOCK_SIZE = 256 * 1024 * 1024;
    public static int HDFS_BUFFER_SIZE = 256 * 1024;

    public static int MIN_REPEAT = 3;
    public static int MAX_SCOPE = 512;
    public static int MAX_SHORT_REPEAT_LENGTH = 10;
    public static float DICT_KEY_SIZE_THRESHOLD = 0.1F;
    public static int INIT_DICT_SIZE = 4096;

    public static String LAYOUT_VERSION_LITERAL = "layout_version";
    public static String CACHE_VERSION_LITERAL = "cache_version";
    public static String CACHE_COORDINATOR_LITERAL = "coordinator";
    public static String CACHE_NODE_STATUS_LITERAL = "node_";
    public static String CACHE_LOCATION_LITERAL = "location_";
    public static int MAX_BLOCK_ID_LEN = 20480;
}
