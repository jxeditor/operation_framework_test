package com.frame.tools.time;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.frame.tools.time.TimesTools.DATE_TIME_FORMATTER_YYYY;
import static com.frame.tools.time.TimesTools.DATE_TIME_FORMATTER_YYYYMM;

/**
 * The type Times dt 相关操作转换工具类
 * dt       日期格式为  yyyy-MM-dd
 * dtMonth  日期格式为  yyyy-MM
 * dt 对应 LocalDate , 只精确到日
 *
 * @author XiaShuai on 2020/4/8.
 */
public class TimesDtTools {
    /**
     * dt e.g 2017-11-06
     */
    public static final String YYYY_MM_DD = "yyyy-MM-dd";
    private static final DateTimeFormatter DATE_TIME_FORMATTER_YYYY_MM_DD = DateTimeFormatter.ofPattern(YYYY_MM_DD);
    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private TimesDtTools() {
    }

    /**
     * 获取当前时间对应 dt.
     *
     * @return the string
     */
    public static String nowDt() {
        return dt(LocalDate.now());
    }

    /**
     * LocalDate -> dt
     *
     * @param localDate localDate
     * @return String string
     */
    public static String dt(LocalDate localDate) {
        return localDate.format(DATE_TIME_FORMATTER_YYYY_MM_DD);
    }

    /**
     * unixSec -> dt
     *
     * @param unixSec the unix sec
     * @return the string
     */
    public static String dt(long unixSec) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(unixSec), ZONE_ID)
                .toLocalDate().format(DATE_TIME_FORMATTER_YYYY_MM_DD);
    }

    /**
     * dt+plusDays -> dt
     *
     * @param dt       the dt
     * @param plusDays the plus days
     * @return the string
     */
    public static String dt(String dt, int plusDays) {
        return dt(localDate(dt, plusDays));
    }

    /**
     * dt+plusDays -> dt
     * 对 24 点执行的小时任务进行时间点校验，如果在00:00 - 00:30 启动任务，则不进行 dt+1 操作
     *
     * @param dt the dt
     * @return the string
     */
    public static String hoursTaskDt(String dt) {
        final long time = System.currentTimeMillis() / 1000;
        final String tomorrow = dt(dt, 1);
        if (time >= unixSec(tomorrow) && time < unixSec(tomorrow) + 1800) {
            return dt;
        } else {
            return tomorrow;
        }
    }

    /**
     * dtEnd - dtStart = 相差天数
     *
     * @param dtEnd   dtEnd
     * @param dtStart dtStart
     * @return 相差天数
     */
    public static long dtDiffDay(String dtEnd, String dtStart) {
        return (unixSec(dtEnd) - unixSec(dtStart)) / TimesTools.DAY_SECS;
    }

    /**
     * dt+plusDays -> LocalDate
     *
     * @param dt       dt
     * @param plusDays plusDays
     * @return LocalDate local date
     */
    public static LocalDate localDate(String dt, int plusDays) {
        return localDate(dt).plusDays(plusDays);
    }

    /**
     * dt -> LocalDate
     *
     * @param dt dt
     * @return LocalDate local date
     */
    public static LocalDate localDate(String dt) {
        return LocalDate.parse(dt, DATE_TIME_FORMATTER_YYYY_MM_DD);
    }

    /**
     * unixSec -> LocalDate
     *
     * @param unixSec the unix sec
     * @return the local date
     */
    public static LocalDate localDate(long unixSec) {
        return TimesDtTools.localDate(dt(unixSec));
    }

    /**
     * dt ->  unixSec
     *
     * @param dt dt
     * @return String long
     */
    public static long unixSec(String dt) {
        return localDate(dt).atStartOfDay().atZone(TimesTools.ZONE_ID).toEpochSecond();
    }

    /**
     * dt -> dtT00:00:00+08
     *
     * @param dt     the dt
     * @param format the format
     * @return the string
     */
    public static String dateIso(String dt, String format) {
        long unixSec = unixSec(dt);
        final SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(unixSec * 1000);
    }

    /**
     * dt -> 自定义 format
     *
     * @param dt     the dt
     * @param format the format
     * @return the string
     */
    public static String format(String dt, String format) {
        final LocalDate localDate = LocalDate.parse(dt, DATE_TIME_FORMATTER_YYYY_MM_DD);
        return localDate.format(DateTimeFormatter.ofPattern(format));
    }

    /**
     * dt + plusMonth -> dtMonth
     *
     * @param dt        dt
     * @param plusMonth -1
     * @return 2017 -09
     */
    public static String dtMonth(String dt, int plusMonth) {
        return localDate(dt).plusMonths(plusMonth).format(DATE_TIME_FORMATTER_YYYYMM);
    }

    /**
     * dt + plusYear -> dtYear
     *
     * @param dt        dt
     * @param plusMonth -1
     * @return 2017 -09 -12
     */
    public static String dtPlusMoth(String dt, int plusMonth) {
        return localDate(dt).plusMonths(plusMonth).format(DATE_TIME_FORMATTER_YYYY_MM_DD);
    }

    /**
     * dt + plusYear -> dtYear
     *
     * @param dt       dt
     * @param plusYear -1
     * @return 2017 -09 -12
     */
    public static String dtPlusYear(String dt, int plusYear) {
        return localDate(dt).plusYears(plusYear).format(DATE_TIME_FORMATTER_YYYY_MM_DD);
    }

    /**
     * dt + plusMonth -> dtMonth + "-01"
     * e.g 2019-01-10 + 2 -> 2019-03-01
     *
     * @param dt        20180315
     * @param plusMonth -1
     * @return 返回计算后 dt 对应月的第一天
     */
    public static String dtMonthFirstDay(String dt, int plusMonth) {
        return dtMonth(dt, plusMonth) + "-01";
    }

    /**
     * dt + plusYear -> dtYear + "-01-01"
     * e.g 2019-01-10 + 2 -> 2021-01-01
     *
     * @param dt       20180315
     * @param plusYear -1
     * @return 返回计算后 dt 对应年的1月1号
     */
    public static String dtYearFirstDay(String dt, int plusYear) {
        return localDate(dt).plusYears(plusYear).format(DATE_TIME_FORMATTER_YYYY) + "-01-01";
    }

    /**
     * (dt + plusWeek) - 与周一相差天数 -> dt
     *
     * @param dt       dt
     * @param plusWeek plusWeek
     * @return dt monday dt
     */
    public static String monday(String dt, int plusWeek) {
        final int dtOfWeek = LocalDate.parse(dt, DATE_TIME_FORMATTER_YYYY_MM_DD).getDayOfWeek().getValue();
        return dt(dt, -dtOfWeek + 1 + plusWeek * 7);
    }

    /**
     * dt + 与周日相差天数 + plusWeek
     *
     * @param dt       dt
     * @param plusWeek plusWeek
     * @return dt sunday dt
     */
    public static String sunday(String dt, int plusWeek) {
        final int dtOfWeek = LocalDate.parse(dt, DATE_TIME_FORMATTER_YYYY_MM_DD).getDayOfWeek().getValue();
        return dt(dt, 7 - dtOfWeek + plusWeek * 7);
    }

    public static void main(String[] args) {
        System.out.println(TimesDtTools.monday("2020-05-03", 0));
        System.out.println(TimesDtTools.sunday("2020-05-03", 0));
        System.out.println(TimesDtTools.monday("2020-05-03", -1));
    }
}
