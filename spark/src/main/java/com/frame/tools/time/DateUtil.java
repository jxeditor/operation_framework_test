package com.frame.tools.time;

import com.frame.tools.lang.NonNull;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

/**
 * 日期工具类.
 * <p>
 * 在不方便使用joda-time时，使用本类降低Date处理的复杂度与性能消耗, 封装Common Lang及移植Jodd的最常用日期方法
 * @author XiaShuai on 2020/4/8.
 */
public class DateUtil {
    /**
     * The constant MILLIS_PER_SECOND.
     * Number of milliseconds in a standard second.
     */
    public static final long MILLIS_PER_SECOND = 1000;
    /**
     * The constant MILLIS_PER_MINUTE.
     * Number of milliseconds in a standard minute.
     */
    public static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
    /**
     * The constant MILLIS_PER_HOUR.
     * Number of milliseconds in a standard hour.
     */
    public static final long MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
    /**
     * The constant MILLIS_PER_DAY.
     * Number of milliseconds in a standard day.
     */
    public static final long MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR;

    private static final int[] MONTH_LENGTH = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    private DateUtil() {
        throw new IllegalStateException("Utility class");
    }

    //////// 日期比较 ///////////

    /**
     * 是否同一天.
     *
     * @param date1 the date 1
     * @param date2 the date 2
     * @return the boolean
     * @see DateUtils#isSameDay(Date, Date) DateUtils#isSameDay(Date, Date)DateUtils#isSameDay(Date, Date)
     */
    public static boolean isSameDay(@NonNull final Date date1, @NonNull final Date date2) {
        return DateUtils.isSameDay(date1, date2);
    }

    /**
     * 是否同一时刻.
     *
     * @param date1 the date 1
     * @param date2 the date 2
     * @return the boolean
     */
    public static boolean isSameTime(@NonNull final Date date1, @NonNull final Date date2) {
        // date.getMillisOf() 比date.getTime()快
        return date1.compareTo(date2) == 0;
    }

    /**
     * 判断日期是否在范围内，包含相等的日期
     *
     * @param date  the date
     * @param start the start
     * @param end   the end
     * @return the boolean
     */
    public static boolean isBetween(@NonNull final Date date, @NonNull final Date start, @NonNull final Date end) {
        if (date == null || start == null || end == null || start.after(end)) {
            throw new IllegalArgumentException("some date parameters is null or dateBein after dateEnd");
        }
        return !date.before(start) && !date.after(end);
    }

    //////////// 往前往后滚动时间//////////////

    /**
     * 加一月
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date addMonths(@NonNull final Date date, int amount) {
        return DateUtils.addMonths(date, amount);
    }

    /**
     * 减一月
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date subMonths(@NonNull final Date date, int amount) {
        return DateUtils.addMonths(date, -amount);
    }

    /**
     * 加一周
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date addWeeks(@NonNull final Date date, int amount) {
        return DateUtils.addWeeks(date, amount);
    }

    /**
     * 减一周
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date subWeeks(@NonNull final Date date, int amount) {
        return DateUtils.addWeeks(date, -amount);
    }

    /**
     * 加一天
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date addDays(@NonNull final Date date, final int amount) {
        return DateUtils.addDays(date, amount);
    }

    /**
     * 减一天
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date subDays(@NonNull final Date date, int amount) {
        return DateUtils.addDays(date, -amount);
    }

    /**
     * 加一小时
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date addHours(@NonNull final Date date, int amount) {
        return DateUtils.addHours(date, amount);
    }

    /**
     * 减一小时
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date subHours(@NonNull final Date date, int amount) {
        return DateUtils.addHours(date, -amount);
    }

    /**
     * 加一分钟
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date addMinutes(@NonNull final Date date, int amount) {
        return DateUtils.addMinutes(date, amount);
    }

    /**
     * 减一分钟
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date subMinutes(@NonNull final Date date, int amount) {
        return DateUtils.addMinutes(date, -amount);
    }

    /**
     * 终于到了，续一秒.
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date addSeconds(@NonNull final Date date, int amount) {
        return DateUtils.addSeconds(date, amount);
    }

    /**
     * 减一秒.
     *
     * @param date   the date
     * @param amount the amount
     * @return the date
     */
    public static Date subSeconds(@NonNull final Date date, int amount) {
        return DateUtils.addSeconds(date, -amount);
    }

    //////////// 直接设置时间//////////////

    /**
     * 设置年份, 公元纪年.
     *
     * @param date   the date
     * @param amount the amount
     * @return the years
     */
    public static Date setYears(@NonNull final Date date, int amount) {
        return DateUtils.setYears(date, amount);
    }

    /**
     * 设置月份, 1-12.
     *
     * @param date   the date
     * @param amount the amount
     * @return the months
     */
    public static Date setMonths(@NonNull final Date date, int amount) {
        if (amount < 1 || amount > 12) {
            throw new IllegalArgumentException("monthOfYear must be in the range[ 1, 12]");
        }
        return DateUtils.setMonths(date, amount - 1);
    }

    /**
     * 设置日期, 1-31.
     *
     * @param date   the date
     * @param amount the amount
     * @return the days
     */
    public static Date setDays(@NonNull final Date date, int amount) {
        return DateUtils.setDays(date, amount);
    }

    /**
     * 设置小时, 0-23.
     *
     * @param date   the date
     * @param amount the amount
     * @return the hours
     */
    public static Date setHours(@NonNull final Date date, int amount) {
        return DateUtils.setHours(date, amount);
    }

    /**
     * 设置分钟, 0-59.
     *
     * @param date   the date
     * @param amount the amount
     * @return the minutes
     */
    public static Date setMinutes(@NonNull final Date date, int amount) {
        return DateUtils.setMinutes(date, amount);
    }

    /**
     * 设置秒, 0-59.
     *
     * @param date   the date
     * @param amount the amount
     * @return the seconds
     */
    public static Date setSeconds(@NonNull final Date date, int amount) {
        return DateUtils.setSeconds(date, amount);
    }

    /**
     * 设置毫秒.
     *
     * @param date   the date
     * @param amount the amount
     * @return the milliseconds
     */
    public static Date setMilliseconds(@NonNull final Date date, int amount) {
        return DateUtils.setMilliseconds(date, amount);
    }

    ///// 获取日期的位置//////

    /**
     * 获得日期是一周的第几天. 已改为中国习惯，1 是Monday，而不是Sundays.
     *
     * @param date the date
     * @return the day of week
     */
    public static int getDayOfWeek(@NonNull final Date date) {
        int result = getWithMondayFirst(date, Calendar.DAY_OF_WEEK);
        return result == 1 ? 7 : result - 1;
    }

    /**
     * 获得日期是一年的第几天，返回值从1开始
     *
     * @param date the date
     * @return the day of year
     */
    public static int getDayOfYear(@NonNull final Date date) {
        return get(date, Calendar.DAY_OF_YEAR);
    }

    /**
     * 获得日期是一月的第几周，返回值从1开始.
     * <p>
     * 开始的一周，只要有一天在那个月里都算. 已改为中国习惯，1 是Monday，而不是Sunday
     *
     * @param date the date
     * @return the week of month
     */
    public static int getWeekOfMonth(@NonNull final Date date) {
        return getWithMondayFirst(date, Calendar.WEEK_OF_MONTH);
    }

    /**
     * 获得日期是一年的第几周，返回值从1开始.
     * <p>
     * 开始的一周，只要有一天在那一年里都算.已改为中国习惯，1 是Monday，而不是Sunday
     *
     * @param date the date
     * @return the week of year
     */
    public static int getWeekOfYear(@NonNull final Date date) {
        return getWithMondayFirst(date, Calendar.WEEK_OF_YEAR);
    }

    private static int get(final Date date, int field) {
        Validate.notNull(date, "The date must not be null");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        return cal.get(field);
    }

    private static int getWithMondayFirst(final Date date, int field) {
        Validate.notNull(date, "The date must not be null");
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.setTime(date);
        return cal.get(field);
    }

    ///// 获得往前往后的日期//////

    /**
     * 2016-11-10 07:33:23, 则返回2016-1-1 00:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date beginOfYear(@NonNull final Date date) {
        return DateUtils.truncate(date, Calendar.YEAR);
    }

    /**
     * 2016-11-10 07:33:23, 则返回2016-12-31 23:59:59.999
     *
     * @param date the date
     * @return the date
     */
    public static Date endOfYear(@NonNull final Date date) {
        return new Date(nextYear(date).getTime() - 1);
    }

    /**
     * 2016-11-10 07:33:23, 则返回2017-1-1 00:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date nextYear(@NonNull final Date date) {
        return DateUtils.ceiling(date, Calendar.YEAR);
    }

    /**
     * 2016-11-10 07:33:23, 则返回2016-11-1 00:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date beginOfMonth(@NonNull final Date date) {
        return DateUtils.truncate(date, Calendar.MONTH);
    }

    /**
     * 2016-11-10 07:33:23, 则返回2016-11-30 23:59:59.999
     *
     * @param date the date
     * @return the date
     */
    public static Date endOfMonth(@NonNull final Date date) {
        return new Date(nextMonth(date).getTime() - 1);
    }

    /**
     * 2016-11-10 07:33:23, 则返回2016-12-1 00:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date nextMonth(@NonNull final Date date) {
        return DateUtils.ceiling(date, Calendar.MONTH);
    }

    /**
     * 2017-1-20 07:33:23, 则返回2017-1-16 00:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date beginOfWeek(@NonNull final Date date) {
        return DateUtils.truncate(DateUtil.subDays(date, DateUtil.getDayOfWeek(date) - 1), Calendar.DATE);
    }

    /**
     * 2017-1-20 07:33:23, 则返回2017-1-22 23:59:59.999
     *
     * @param date the date
     * @return the date
     */
    public static Date endOfWeek(@NonNull final Date date) {
        return new Date(nextWeek(date).getTime() - 1);
    }

    /**
     * 2017-1-23 07:33:23, 则返回2017-1-22 00:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date nextWeek(@NonNull final Date date) {
        return DateUtils.truncate(DateUtil.addDays(date, 8 - DateUtil.getDayOfWeek(date)), Calendar.DATE);
    }

    /**
     * 2016-11-10 07:33:23, 则返回2016-11-10 00:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date beginOfDate(@NonNull final Date date) {
        return DateUtils.truncate(date, Calendar.DATE);
    }

    /**
     * 2017-1-23 07:33:23, 则返回2017-1-23 23:59:59.999
     *
     * @param date the date
     * @return the date
     */
    public static Date endOfDate(@NonNull final Date date) {
        return new Date(nextDate(date).getTime() - 1);
    }

    /**
     * 2016-11-10 07:33:23, 则返回2016-11-11 00:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date nextDate(@NonNull final Date date) {
        return DateUtils.ceiling(date, Calendar.DATE);
    }

    /**
     * 2016-12-10 07:33:23, 则返回2016-12-10 07:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date beginOfHour(@NonNull final Date date) {
        return DateUtils.truncate(date, Calendar.HOUR_OF_DAY);
    }

    /**
     * 2017-1-23 07:33:23, 则返回2017-1-23 07:59:59.999
     *
     * @param date the date
     * @return the date
     */
    public static Date endOfHour(@NonNull final Date date) {
        return new Date(nextHour(date).getTime() - 1);
    }

    /**
     * 2016-12-10 07:33:23, 则返回2016-12-10 08:00:00
     *
     * @param date the date
     * @return the date
     */
    public static Date nextHour(@NonNull final Date date) {
        return DateUtils.ceiling(date, Calendar.HOUR_OF_DAY);
    }

    /**
     * 2016-12-10 07:33:23, 则返回2016-12-10 07:33:00
     *
     * @param date the date
     * @return the date
     */
    public static Date beginOfMinute(@NonNull final Date date) {
        return DateUtils.truncate(date, Calendar.MINUTE);
    }

    /**
     * 2017-1-23 07:33:23, 则返回2017-1-23 07:33:59.999
     *
     * @param date the date
     * @return the date
     */
    public static Date endOfMinute(@NonNull final Date date) {
        return new Date(nextMinute(date).getTime() - 1);
    }

    /**
     * 2016-12-10 07:33:23, 则返回2016-12-10 07:34:00
     *
     * @param date the date
     * @return the date
     */
    public static Date nextMinute(@NonNull final Date date) {
        return DateUtils.ceiling(date, Calendar.MINUTE);
    }

    ////// 闰年及每月天数///////

    /**
     * 是否闰年.
     *
     * @param date the date
     * @return the boolean
     */
    public static boolean isLeapYear(@NonNull final Date date) {
        return isLeapYear(get(date, Calendar.YEAR));
    }

    /**
     * 是否闰年，copy from Jodd Core的TimeUtil
     * <p>
     * 参数是公元计数, 如2016
     *
     * @param y the y
     * @return the boolean
     */
    public static boolean isLeapYear(int y) {
        boolean result = false;

        if (((y % 4) == 0) && // must be divisible by 4...
                ((y < 1582) || // and either before reform year...
                        ((y % 100) != 0) || // or not a century...
                        ((y % 400) == 0))) { // or a multiple of 400...
            result = true; // for leap year.
        }
        return result;
    }

    /**
     * 获取某个月有多少天, 考虑闰年等因数, 移植Jodd Core的TimeUtil
     *
     * @param date the date
     * @return the month length
     */
    public static int getMonthLength(@NonNull final Date date) {
        int year = get(date, Calendar.YEAR);
        int month = get(date, Calendar.MONTH);
        return getMonthLength(year, month);
    }

    /**
     * 获取某个月有多少天, 考虑闰年等因数, 移植Jodd Core的TimeUtil
     *
     * @param year  the year
     * @param month the month
     * @return the month length
     */
    public static int getMonthLength(int year, int month) {

        if ((month < 1) || (month > 12)) {
            throw new IllegalArgumentException("Invalid month: " + month);
        }
        if (month == 2) {
            return isLeapYear(year) ? 29 : 28;
        }

        return MONTH_LENGTH[month];
    }
}
