package org.joisen.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * SimpleDataFormat 涉及线程安全问题
 * @Author Joisen
 * @Date 2023/2/3 14:33
 * @Version 1.0
 */
public class DateFormatUtil {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将时间字符串(2023-01-01 00:00:00)转换为时间戳
     * 如果时间字符串不是完整格式(2023-01-01), 需要转换为完整格式(在后面加上 00:00:00)
     */
    public static Long toTs(String dtStr, boolean isFull) {

        LocalDateTime localDateTime = null;
        if (!isFull) {
            dtStr = dtStr + " 00:00:00";
        }
        localDateTime = LocalDateTime.parse(dtStr, dtfFull);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static Long toTs(String dtStr) {
        return toTs(dtStr, false);
    }

    /**
     * 将时间戳转换为时间字符串(不完整的)
     */
    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }
    /**
     * 将时间戳转换为时间字符串(完整的)
     */
    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    public static void main(String[] args) {
        System.out.println(toYmdHms(System.currentTimeMillis()));
    }


}
