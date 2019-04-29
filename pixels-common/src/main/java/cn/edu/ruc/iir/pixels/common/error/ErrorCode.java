package cn.edu.ruc.iir.pixels.common.error;

/**
 * Created at: 19-4-19
 * Author: hank
 */
public class ErrorCode
{
    // begin error code for metadata rpc
    public static final int METADATA_SCHEMA_NOT_FOUND = 10000;
    public static final int METADATA_TABLE_NOT_FOUND = 10001;
    public static final int METADATA_LAYOUT_NOT_FOUND = 10002;
    public static final int METADATA_COLUMN_NOT_FOUND = 10003;
    public static final int METADATA_LAYOUT_DUPLICATED = 10004;
    public static final int METADATA_SCHEMA_EXIST = 10005;
    public static final int METADATA_TABLE_EXIST = 10006;
    public static final int METADATA_DELETE_SCHEMA_FAILED = 10007;
    public static final int METADATA_DELETE_TABLE_FAILED = 10008;
    public static final int METADATA_ADD_COUMNS_FAILED = 10009;
    // end error code for metadata rpc
}
