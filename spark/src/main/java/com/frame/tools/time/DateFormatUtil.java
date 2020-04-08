package com.frame.tools.time;

import com.frame.tools.lang.NonNull;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Date;

/**
 * Date的parse()与format(), 采用Apache Common Lang中线程安全, 性能更佳的FastDateFormat
 * <p>
 * 注意Common Lang版本，3.5版才使用StringBuilder，3.4及以前使用StringBuffer.
 * <p>
 * 1. 常用格式的FastDateFormat定义, 常用格式直接使用这些FastDateFormat
 * <p>
 * 2. 日期格式不固定时的String<->Date 转换函数.
 * <p>
 * 3. 打印时间间隔，如"01:10:10"，以及用户友好的版本，比如"刚刚"，"10分钟前"
 * @author XiaShuai on 2020/4/8.
 */
public final class DateFormatUtil {
    /**
     * The constant PATTERN_ISO.
     * 以T分隔日期和时间，并带时区信息，符合ISO8601规范
     */
    public static final String PATTERN_ISO = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
    /**
     * The constant PATTERN_ISO_ON_SECOND.
     * <p>
     * 格式化 ex
     * JAVA ：2018-10-23 13:54:57.871+08:00
     * 经过时区处理
     * beat-GO语言处理后 "2018-10-23T05:53:40.000Z"
     */
    public static final String PATTERN_ELASTIC_TIMESTAMP = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";

    public static final String PATTERN_ISO_SS_X = "yyyy-MM-dd'T'HH:mm:ssX";

    /**
     * The constant PATTERN_ISO_ON_DATE.
     */
    public static final String PATTERN_ISO_ON_DATE = "yyyy-MM-dd";
    /**
     * The constant PATTERN_DEFAULT.
     * 以空格分隔日期和时间，不带时区信息
     */
    public static final String PATTERN_DEFAULT = "yyyy-MM-dd HH:mm:ss.SSS";
    /**
     * The constant PATTERN_DEFAULT_ON_SECOND.
     */
    public static final String PATTERN_DEFAULT_ON_SECOND = "yyyy-MM-dd HH:mm:ss";
    /**
     * The constant ISO_FORMAT.
     * 以T分隔日期和时间，并带时区信息，符合ISO8601规范
     */
    public static final FastDateFormat ISO_FORMAT = FastDateFormat.getInstance(PATTERN_ISO);

    // 使用工厂方法FastDateFormat.getInstance(), 从缓存中获取实例
    /**
     * The constant ISO_ON_DATE_FORMAT.
     */
    public static final FastDateFormat ISO_ON_DATE_FORMAT = FastDateFormat.getInstance(PATTERN_ISO_ON_DATE);
    /**
     * The constant DEFAULT_FORMAT.
     * 以空格分隔日期和时间，不带时区信息
     */
    public static final FastDateFormat DEFAULT_FORMAT = FastDateFormat.getInstance(PATTERN_DEFAULT);
    /**
     * The constant DEFAULT_ON_SECOND_FORMAT.
     */
    public static final FastDateFormat DEFAULT_ON_SECOND_FORMAT = FastDateFormat.getInstance(PATTERN_DEFAULT_ON_SECOND);

    private DateFormatUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * 分析日期字符串, 仅用于pattern不固定的情况.
     * <p>
     * 否则直接使用DateFormats中封装好的FastDateFormat.
     * <p>
     * FastDateFormat.getInstance()已经做了缓存，不会每次创建对象，但直接使用对象仍然能减少在缓存中的查找.
     *
     * @param pattern    the pattern
     * @param dateString the date string
     * @return the date
     * @throws ParseException the parse exception
     */
    public static Date parseDate(@NonNull String pattern, @NonNull String dateString) throws ParseException {
        return FastDateFormat.getInstance(pattern).parse(dateString);
    }

    /**
     * 格式化日期, 仅用于pattern不固定的情况.
     * <p>
     * 否则直接使用本类中封装好的FastDateFormat.
     * <p>
     * FastDateFormat.getInstance()已经做了缓存，不会每次创建对象，但直接使用对象仍然能减少在缓存中的查找.
     *
     * @param pattern the pattern
     * @param date    the date
     * @return the string
     */
    public static String formatDate(@NonNull String pattern, @NonNull Date date) {
        return FastDateFormat.getInstance(pattern).format(date);
    }

    /**
     * 格式化日期, 仅用于不固定pattern不固定的情况.
     * <p>
     * 否否则直接使用本类中封装好的FastDateFormat.
     * <p>
     * FastDateFormat.getInstance()已经做了缓存，不会每次创建对象，但直接使用对象仍然能减少在缓存中的查找.
     *
     * @param pattern the pattern
     * @param date    the date
     * @return the string
     */
    public static String formatDate(@NonNull String pattern, long date) {
        return FastDateFormat.getInstance(pattern).format(date);
    }

    /////// 格式化间隔时间/////////

    /**
     * 按HH:mm:ss.SSS格式，格式化时间间隔.
     * <p>
     * endDate必须大于startDate，间隔可大于1天，
     *
     * @param startDate the start date
     * @param endDate   the end date
     * @return the string
     * @see DurationFormatUtils
     */
    public static String formatDuration(@NonNull Date startDate, @NonNull Date endDate) {
        return DurationFormatUtils.formatDurationHMS(endDate.getTime() - startDate.getTime());
    }

    /**
     * 按HH:mm:ss.SSS格式，格式化时间间隔
     * <p>
     * 单位为毫秒，必须大于0，可大于1天
     *
     * @param durationMillis the duration millis
     * @return the string
     * @see DurationFormatUtils
     */
    public static String formatDuration(long durationMillis) {
        return DurationFormatUtils.formatDurationHMS(durationMillis);
    }

    /**
     * 按HH:mm:ss格式，格式化时间间隔
     * <p>
     * endDate必须大于startDate，间隔可大于1天
     *
     * @param startDate the start date
     * @param endDate   the end date
     * @return the string
     * @see DurationFormatUtils
     */
    public static String formatDurationOnSecond(@NonNull Date startDate, @NonNull Date endDate) {
        return DurationFormatUtils.formatDuration(endDate.getTime() - startDate.getTime(), "HH:mm:ss");
    }

    /**
     * 按HH:mm:ss格式，格式化时间间隔
     * <p>
     * 单位为毫秒，必须大于0，可大于1天
     *
     * @param durationMillis the duration millis
     * @return the string
     * @see DurationFormatUtils
     */
    public static String formatDurationOnSecond(long durationMillis) {
        return DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss");
    }

    //////// 打印用于页面显示的用户友好，与当前时间比的时间差

    /**
     * 打印用户友好的，与当前时间相比的时间差，如刚刚，5分钟前，今天XXX，昨天XXX
     * <p>
     * copy from AndroidUtilCode
     *
     * @param date the date
     * @return the string
     */
    public static String formatFriendlyTimeSpanByNow(@NonNull Date date) {
        return formatFriendlyTimeSpanByNow(date.getTime());
    }

    /**
     * 打印用户友好的，与当前时间相比的时间差，如刚刚，5分钟前，今天XXX，昨天XXX
     * <p>
     * copy from AndroidUtilCode
     *
     * @param timeStampMillis the time stamp millis
     * @return the string
     */
    public static String formatFriendlyTimeSpanByNow(long timeStampMillis) {
        long now = ClockUtil.currentTimeMillis();
        long span = now - timeStampMillis;
        if (span < 0) {
            // 'c' 日期和时间，被格式化为 "%ta %tb %td %tT %tZ %tY"，例如 "Sun Jul 20 16:17:00 EDT 1969"。
            return String.format("%tc", timeStampMillis);
        }
        if (span < DateUtil.MILLIS_PER_SECOND) {
            return "刚刚";
        } else if (span < DateUtil.MILLIS_PER_MINUTE) {
            return String.format("%d秒前", span / DateUtil.MILLIS_PER_SECOND);
        } else if (span < DateUtil.MILLIS_PER_HOUR) {
            return String.format("%d分钟前", span / DateUtil.MILLIS_PER_MINUTE);
        }
        // 获取当天00:00
        long wee = DateUtil.beginOfDate(new Date(now)).getTime();
        if (timeStampMillis >= wee) {
            // 'R' 24 小时制的时间，被格式化为 "%tH:%tM"
            return String.format("今天%tR", timeStampMillis);
        } else if (timeStampMillis >= wee - DateUtil.MILLIS_PER_DAY) {
            return String.format("昨天%tR", timeStampMillis);
        } else {
            // 'F' ISO 8601 格式的完整日期，被格式化为 "%tY-%tm-%td"。
            return String.format("%tF", timeStampMillis);
        }
    }

    public static String formatFriendlyDurationMillis(long millis) {

        return new FormatFriendlyHelper(millis).format();
    }

    public static String formatFriendlyDurationSecond(long startSecond, long endSecond) {
        return formatFriendlyDurationMillis((endSecond - startSecond) * 1000L);
    }

    public static String formatFriendlyDurationMillis(long startMillis, long endMillis) {
        return formatFriendlyDurationMillis(endMillis - startMillis);
    }


    /**
     * 格式化时间间隔为友好单位 {@linkplain #millis 毫秒时间间隔}
     * 2d 10h 5m 30s
     * 5m 30s
     * 30s
     * <p>
     * ~~~~~~~~~~~
     * final String invoke = formatFriendlyDuration(
     * 2 * DateUtil.MILLIS_PER_DAY +
     * 10 * DateUtil.MILLIS_PER_HOUR +
     * 10 * DateUtil.MILLIS_PER_MINUTE +
     * 30 * DateUtil.MILLIS_PER_SECOND);
     * System.out.println(invoke);
     */
    private static class FormatFriendlyHelper {

        private long millis;
        FormatFriendlyHelper(long millis) {
            this.millis = millis;
        }

        String format() {

            if (this.millis < 0) {
                return "-";
            }

            long day = millis / DateUtil.MILLIS_PER_DAY;
            long hour = (millis % DateUtil.MILLIS_PER_DAY) / DateUtil.MILLIS_PER_HOUR;
            long minute = (millis % DateUtil.MILLIS_PER_HOUR) / DateUtil.MILLIS_PER_MINUTE;
            long second = (millis % DateUtil.MILLIS_PER_MINUTE) / DateUtil.MILLIS_PER_SECOND;

            final StringBuilder builder = new StringBuilder();
            if (day != 0) {
                builder.append(day).append("d ");
            }
            if (hour != 0) {
                builder.append(hour).append("h ");
            }
            if (minute != 0) {
                builder.append(minute).append("m ");
            }
            if (second != 0) {
                builder.append(second).append("s ");
            }
            return builder.toString().trim();
        }

    }
}
