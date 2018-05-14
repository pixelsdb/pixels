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

    public static int MIN_REPEAT= 3;
    public static int MAX_SCOPE = 512;
    public static int MAX_SHORT_REPEAT_LENGTH = 10;
    public static float DICT_KEY_SIZE_THRESHOLD = 0.1F;
    public static int INIT_DICT_SIZE = 4096;
}
