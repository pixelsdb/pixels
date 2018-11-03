package cn.edu.ruc.iir.pixels.common.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.common
 * @ClassName: DateUtil
 * @Description: To sum some static useful functions
 * @author: Tao
 * @date: Create in 2017-07-28 7:11
 **/
public class DateUtil {
    private static AtomicInteger count = new AtomicInteger(0);

    public static String formatTime(Long time) {

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(new Date(time));
    }

    public static String formatTime(Date time) {

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(time);
    }

    public static String getCurTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");//set the style
        return df.format(new Date()) + "_" + count.getAndIncrement();
    }

}
