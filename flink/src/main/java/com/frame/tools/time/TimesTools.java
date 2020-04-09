package com.frame.tools.time;

import com.frame.tools.lang.Nullable;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * @author XiaShuai on 2020/4/8.
 */
public class TimesTools {
    /**
     * 2017-11-06T13:55:55+08
     */
    public static final String YYYY_MM_DD_T_HH_MM_SS_X = "yyyy-MM-dd'T'HH:mm:ssX";
    public static final String YYYY_MM = "yyyy-MM";
    public static final String YYYY = "yyyy";
    /**
     * 20171106
     */
    public static final String YYYYMMDD = "yyyyMMdd";
    /**
     * 2017-11-06
     */
    public static final String YYYY_MM_DD = "yyyy-MM-dd";
    /**
     * 一天中的小时数 0-23
     */
    public static final String H = "H";
    /**
     * 1330
     */
    public static final String HHMM = "HHmm";

    public static final DateTimeFormatter DATE_TIME_FORMATTER_YYYYMM = DateTimeFormatter.ofPattern(YYYY_MM);
    public static final DateTimeFormatter DATE_TIME_FORMATTER_YYYY = DateTimeFormatter.ofPattern(YYYY);
    public static final ZoneId ZONE_ID = ZoneId.systemDefault();
    public static final TimeZone DEFAULT_TIMEZONE = TimeZone.getDefault();
    public static final long MINUTE_SECS = 60L;
    public static final long HOURS_SECS = MINUTE_SECS * 60L;
    public static final long DAY_SECS = HOURS_SECS * 24L;
    public static final long MONTH_SECS = DAY_SECS * 30L;
    public static final long YEAR_SECS = MONTH_SECS * 12L;
    public static final long YEAR_5_SECS = MONTH_SECS * 12L * 5;
    public static final long WEEK_SECS = DAY_SECS * 7L;

    private TimesTools() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * @return 当前 s 时间戳
     */
    public static long nowUnixSec() {

        return (nowUnixMillis() / 1000);
    }

    /**
     * @return 当前 ms 时间戳
     */
    public static long nowUnixMillis() {

        return System.currentTimeMillis();
    }

    /**
     * @return 当前整30分钟时刻 s 时间戳
     */
    public static long now30MinUnixSec() {

        final long l = nowUnixSec();
        return l - (l % (60 * 30));
    }

    /**
     * 延迟时间计算
     *
     * @param dataUnix UNIX_秒时间戳
     * @return 当前时间 - 数据时间
     */
    public static long delaySecCalculate(@Nullable Long dataUnix) {
        if (dataUnix == null) {
            return 0;
        }
        return nowUnixSec() - dataUnix;
    }
}
