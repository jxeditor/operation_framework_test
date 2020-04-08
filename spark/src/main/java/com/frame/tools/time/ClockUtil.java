package com.frame.tools.time;

import java.util.Date;

/**
 * 日期提供者, 使用它而不是直接取得系统时间, 方便测试.
 * <p>
 * 平时使用DEFAULT，测试时替换为DummyClock，可准确控制时间变化而不用Thread.sleep()等待时间流逝.
 * @author XiaShuai on 2020/4/8.
 */
public class ClockUtil {
    private static Clock instance = new DefaultClock();

    /**
     * 计算流逝的时间
     *
     * @param beginTime the begin time
     * @return the long
     */
    public static long elapsedTime(long beginTime) {
        return currentTimeMillis() - beginTime;
    }

    /**
     * 切换为DummyClock，使用系统时间为初始时间, 单个测试完成后需要调用useDefaultClock()切换回去.
     *
     * @return the dummy clock
     */
    public static synchronized DummyClock useDummyClock() {
        instance = new DummyClock();
        return (DummyClock) instance;
    }

    /**
     * 切换为DummyClock，单个测试完成后需要调用useDefaultClock()切换回去.
     *
     * @param timeStampMills the time stamp mills
     * @return the dummy clock
     */
    public static synchronized DummyClock useDummyClock(long timeStampMills) {
        instance = new DummyClock(timeStampMills);
        return (DummyClock) instance;
    }

    /**
     * 切换为DummyClock，单个测试完成后需要调用useDefaultClock()切换回去.
     *
     * @param date the date
     * @return the dummy clock
     */
    public static synchronized DummyClock useDummyClock(Date date) {
        instance = new DummyClock(date);
        return (DummyClock) instance;
    }

    /**
     * 重置为默认Clock
     */
    public static synchronized void useDefaultClock() {
        instance = new DefaultClock();
    }

    /**
     * 系统当前时间
     *
     * @return the date
     */
    public static Date currentDate() {
        return instance.currentDate();
    }

    /**
     * 系统当前时间戳
     *
     * @return the long
     */
    public static long currentTimeMillis() {
        return instance.currentTimeMillis();
    }

    /**
     * 操作系统启动到现在的纳秒数，与系统时间是完全独立的两个时间体系
     *
     * @return the long
     */
    public static long nanoTime() {
        return instance.nanoTime();
    }

    /**
     * The interface Clock.
     */
    public interface Clock {

        /**
         * 系统当前时间
         *
         * @return the date
         */
        Date currentDate();

        /**
         * 系统当前时间戳 毫秒
         *
         * @return the long
         */
        long currentTimeMillis();

        /**
         * 操作系统启动到现在的纳秒数，与系统时间是完全独立的两个时间体系
         *
         * @return the long
         */
        long nanoTime();
    }

    /**
     * 默认时间提供者，返回当前的时间，线程安全。
     */
    public static class DefaultClock implements Clock {

        @Override
        public Date currentDate() {
            return new Date();
        }

        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public long nanoTime() {
            return System.nanoTime();
        }
    }

    /**
     * 可配置的时间提供者，用于测试.
     */
    public static class DummyClock implements Clock {

        private long time;
        private long nanoTme;

        /**
         * Instantiates a new Dummy clock.
         */
        public DummyClock() {
            this(System.currentTimeMillis());
        }

        /**
         * Instantiates a new Dummy clock.
         *
         * @param date the date
         */
        public DummyClock(Date date) {
            this(date.getTime());
        }

        /**
         * Instantiates a new Dummy clock.
         *
         * @param time the time
         */
        public DummyClock(long time) {
            this.time = time;
            this.nanoTme = System.nanoTime();
        }

        @Override
        public Date currentDate() {
            return new Date(time);
        }

        @Override
        public long currentTimeMillis() {
            return time;
        }

        /**
         * 获取nanotime
         */
        @Override
        public long nanoTime() {
            return nanoTme;
        }

        /**
         * 重新设置日期.
         *
         * @param newDate the new date
         */
        public void updateNow(Date newDate) {
            time = newDate.getTime();
        }

        /**
         * 重新设置时间.
         *
         * @param newTime the new time
         */
        public void updateNow(long newTime) {
            this.time = newTime;
        }

        /**
         * 滚动时间.
         *
         * @param millis the millis
         */
        public void increaseTime(int millis) {
            time += millis;
        }

        /**
         * 滚动时间.
         *
         * @param millis the millis
         */
        public void decreaseTime(int millis) {
            time -= millis;
        }

        /**
         * 设置nanotime.
         *
         * @param nanoTime the nano time
         */
        public void setNanoTime(long nanoTime) {
            this.nanoTme = nanoTime;
        }
    }
}
