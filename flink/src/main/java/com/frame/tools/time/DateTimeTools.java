package com.frame.tools.time;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * @author XiaShuai on 2020/4/8.
 */
public final class DateTimeTools {
    /**
     * The constant SYSTEM_DEFAULT_ZONE_ID.
     */
    public static final ZoneId SYSTEM_DEFAULT_ZONE_ID = ZoneId.systemDefault();
    /**
     * The constant SYSTEM_DEFAULT_ZONE_OFFSET.
     */
    public static final ZoneOffset SYSTEM_DEFAULT_ZONE_OFFSET = OffsetDateTime.now().getOffset();
    private static final DateTimeFormatter DATE_TIME_FORMATTER_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DateTimeTools() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Now unix sec long.
     *
     * @return 当前 s 时间戳
     */
    public static long nowUnixSec() {

        return (nowUnixMillis() / 1000);
    }

    /**
     * Now unix millis long.
     *
     * @return 当前 ms 时间戳
     */
    public static long nowUnixMillis() {

        return System.currentTimeMillis();
    }

    /**
     * unixSec -> 2017-10-10 10:00:00
     *
     * @param unixSec unixSec
     * @return String string
     */
    public static String unixSecFormatDateTime(long unixSec) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(unixSec), SYSTEM_DEFAULT_ZONE_ID).format(DATE_TIME_FORMATTER_DATE_TIME);
    }

    /**
     * 对应时间单位，小于该单位的数据进行取整操作.
     *
     * @param chronoUnit the chrono unit
     * @param time       the time
     * @return the local date time
     */
    public static LocalDateTime floorChronoUnit(ChronoUnit chronoUnit, LocalDateTime time) {
        final LocalDateTime r;
        switch (chronoUnit) {
            case MINUTES:
                r = floorMinutes(time);
                break;
            case HOURS:
                r = floorHours(time);
                break;
            case DAYS:
                r = floorDays(time);
                break;
            case WEEKS:
                r = floorWeeks(time);
                break;
            case MONTHS:
                r = floorMonths(time);
                break;
            case YEARS:
                r = floorYears(time);
                break;
            default:
                throw new NullPointerException("ChronoUnit miss");
        }
        return r;
    }

    private static LocalDateTime floorMinutes(LocalDateTime time) {
        return time
                .withSecond(0)
                .withNano(0);
    }

    private static LocalDateTime floorHours(LocalDateTime time) {
        return floorMinutes(time)
                .withMinute(0);
    }

    private static LocalDateTime floorDays(LocalDateTime time) {
        return floorHours(time)
                .withHour(0);
    }

    private static LocalDateTime floorWeeks(LocalDateTime time) {
        return floorDays(time);
    }

    private static LocalDateTime floorMonths(LocalDateTime time) {
        return floorDays(time)
                .withDayOfMonth(0);
    }

    private static LocalDateTime floorYears(LocalDateTime time) {
        return floorDays(time)
                .withDayOfYear(1);
    }
}
