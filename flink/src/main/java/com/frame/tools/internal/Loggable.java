package com.frame.tools.internal;

import org.slf4j.Logger;
import org.slf4j.Marker;

/**
 * 该类仅支持 org.slf4j 相关依赖
 * 需要进行日志打印类实现该接口即可，默认情况无需重写该类中 logXX 方法
 * Logger name = {@linkplain #logger() 返回固定Logger} ，默认为 Loggable
 *
 * @author XiaShuai on 2020/4/8.
 */
public interface Loggable {
    /**
     * Logger logger.
     *
     * @return the logger
     */
    default Logger logger() {
        return LoggableHelper.LOGGABLE;
    }

    /**
     * Log trace.
     *
     * @param msg the msg
     */
    default void logTrace(String msg) {
        this.logger().trace(msg);
    }

    /**
     * Log trace.
     *
     * @param format the format
     * @param arg    the arg
     */
    default void logTrace(String format, Object arg) {
        this.logger().trace(format, arg);
    }

    /**
     * Log trace.
     *
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logTrace(String format, Object arg1, Object arg2) {
        this.logger().trace(format, arg1, arg2);
    }

    /**
     * Log trace.
     *
     * @param format    the format
     * @param arguments the arguments
     */
    default void logTrace(String format, Object... arguments) {
        this.logger().trace(format, arguments);
    }

    /**
     * Log trace.
     *
     * @param msg the msg
     * @param t   the t
     */
    default void logTrace(String msg, Throwable t) {
        this.logger().trace(msg, t);
    }

    /**
     * Log trace.
     *
     * @param marker the marker
     * @param msg    the msg
     */
    default void logTrace(Marker marker, String msg) {
        this.logger().trace(marker, msg);
    }

    /**
     * Log trace.
     *
     * @param marker the marker
     * @param format the format
     * @param arg    the arg
     */
    default void logTrace(Marker marker, String format, Object arg) {
        this.logger().trace(marker, format, arg);
    }

    /**
     * Log trace.
     *
     * @param marker the marker
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logTrace(Marker marker, String format, Object arg1, Object arg2) {
        this.logger().trace(marker, format, arg1, arg2);
    }

    /**
     * Log trace.
     *
     * @param marker   the marker
     * @param format   the format
     * @param argArray the arg array
     */
    default void logTrace(Marker marker, String format, Object... argArray) {
        this.logger().trace(marker, format, argArray);
    }

    /**
     * Log trace.
     *
     * @param marker the marker
     * @param msg    the msg
     * @param t      the t
     */
    default void logTrace(Marker marker, String msg, Throwable t) {
        this.logger().trace(marker, msg, t);
    }

    /**
     * Log debug.
     *
     * @param msg the msg
     */
    default void logDebug(String msg) {
        this.logger().debug(msg);
    }

    /**
     * Log debug.
     *
     * @param format the format
     * @param arg    the arg
     */
    default void logDebug(String format, Object arg) {
        this.logger().debug(format, arg);
    }

    /**
     * Log debug.
     *
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logDebug(String format, Object arg1, Object arg2) {
        this.logger().debug(format, arg1, arg2);
    }

    /**
     * Log debug.
     *
     * @param format    the format
     * @param arguments the arguments
     */
    default void logDebug(String format, Object... arguments) {
        this.logger().debug(format, arguments);
    }

    /**
     * Log debug.
     *
     * @param msg the msg
     * @param t   the t
     */
    default void logDebug(String msg, Throwable t) {
        this.logger().debug(msg, t);
    }


    /**
     * Log debug.
     *
     * @param marker the marker
     * @param msg    the msg
     */
    default void logDebug(Marker marker, String msg) {
        this.logger().debug(marker, msg);
    }

    /**
     * Log debug.
     *
     * @param marker the marker
     * @param format the format
     * @param arg    the arg
     */
    default void logDebug(Marker marker, String format, Object arg) {
        this.logger().debug(marker, format, arg);
    }

    /**
     * Log debug.
     *
     * @param marker the marker
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logDebug(Marker marker, String format, Object arg1, Object arg2) {
        this.logger().debug(marker, format, arg1, arg2);
    }

    /**
     * Log debug.
     *
     * @param marker    the marker
     * @param format    the format
     * @param arguments the arguments
     */
    default void logDebug(Marker marker, String format, Object... arguments) {
        this.logger().debug(marker, format, arguments);
    }

    /**
     * Log debug.
     *
     * @param marker the marker
     * @param msg    the msg
     * @param t      the t
     */
    default void logDebug(Marker marker, String msg, Throwable t) {
        this.logger().debug(marker, msg, t);
    }

    /**
     * Log info.
     *
     * @param msg the msg
     */
    default void logInfo(String msg) {
        this.logger().info(msg);
    }

    /**
     * Log info.
     *
     * @param format the format
     * @param arg    the arg
     */
    default void logInfo(String format, Object arg) {
        this.logger().info(format, arg);
    }

    /**
     * Log info.
     *
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logInfo(String format, Object arg1, Object arg2) {
        this.logger().info(format, arg1, arg2);
    }

    /**
     * Log info.
     *
     * @param format    the format
     * @param arguments the arguments
     */
    default void logInfo(String format, Object... arguments) {
        this.logger().info(format, arguments);
    }

    /**
     * Log info.
     *
     * @param msg the msg
     * @param t   the t
     */
    default void logInfo(String msg, Throwable t) {
        this.logger().info(msg, t);
    }

    /**
     * Log info.
     *
     * @param marker the marker
     * @param msg    the msg
     */
    default void logInfo(Marker marker, String msg) {
        this.logger().info(marker, msg);
    }

    /**
     * Log info.
     *
     * @param marker the marker
     * @param format the format
     * @param arg    the arg
     */
    default void logInfo(Marker marker, String format, Object arg) {
        this.logger().info(marker, format, arg);
    }

    /**
     * Log info.
     *
     * @param marker the marker
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logInfo(Marker marker, String format, Object arg1, Object arg2) {
        this.logger().info(marker, format, arg1, arg2);
    }

    /**
     * Log info.
     *
     * @param marker    the marker
     * @param format    the format
     * @param arguments the arguments
     */
    default void logInfo(Marker marker, String format, Object... arguments) {
        this.logger().info(marker, format, arguments);
    }

    /**
     * Log info.
     *
     * @param marker the marker
     * @param msg    the msg
     * @param t      the t
     */
    default void logInfo(Marker marker, String msg, Throwable t) {
        this.logger().info(marker, msg, t);
    }

    /**
     * Log warn.
     *
     * @param msg the msg
     */
    default void logWarn(String msg) {
        this.logger().warn(msg);
    }

    /**
     * Log warn.
     *
     * @param format the format
     * @param arg    the arg
     */
    default void logWarn(String format, Object arg) {
        this.logger().warn(format, arg);
    }

    /**
     * Log warn.
     *
     * @param format    the format
     * @param arguments the arguments
     */
    default void logWarn(String format, Object... arguments) {
        this.logger().warn(format, arguments);
    }

    /**
     * Log warn.
     *
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logWarn(String format, Object arg1, Object arg2) {
        this.logger().warn(format, arg1, arg2);
    }

    /**
     * Log warn.
     *
     * @param msg the msg
     * @param t   the t
     */
    default void logWarn(String msg, Throwable t) {
        this.logger().warn(msg, t);
    }

    /**
     * Log warn.
     *
     * @param marker the marker
     * @param msg    the msg
     */
    default void logWarn(Marker marker, String msg) {
        this.logger().warn(marker, msg);
    }

    /**
     * Log warn.
     *
     * @param marker the marker
     * @param format the format
     * @param arg    the arg
     */
    default void logWarn(Marker marker, String format, Object arg) {
        this.logger().warn(marker, format, arg);
    }

    /**
     * Log warn.
     *
     * @param marker the marker
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logWarn(Marker marker, String format, Object arg1, Object arg2) {
        this.logger().warn(marker, format, arg1, arg2);
    }

    /**
     * Log warn.
     *
     * @param marker    the marker
     * @param format    the format
     * @param arguments the arguments
     */
    default void logWarn(Marker marker, String format, Object... arguments) {
        this.logger().warn(marker, format, arguments);
    }

    /**
     * Log warn.
     *
     * @param marker the marker
     * @param msg    the msg
     * @param t      the t
     */
    default void logWarn(Marker marker, String msg, Throwable t) {
        this.logger().warn(marker, msg, t);
    }

    /**
     * Log error.
     *
     * @param msg the msg
     */
    default void logError(String msg) {
        this.logger().error(msg);
    }

    /**
     * Log error.
     *
     * @param format the format
     * @param arg    the arg
     */
    default void logError(String format, Object arg) {
        this.logger().error(format, arg);
    }

    /**
     * Log error.
     *
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logError(String format, Object arg1, Object arg2) {
        this.logger().error(format, arg1, arg2);
    }

    /**
     * Log error.
     *
     * @param format    the format
     * @param arguments the arguments
     */
    default void logError(String format, Object... arguments) {
        this.logger().error(format, arguments);
    }

    /**
     * Log error.
     *
     * @param msg the msg
     * @param t   the t
     */
    default void logError(String msg, Throwable t) {
        this.logger().error(msg, t);
    }

    /**
     * Log error.
     *
     * @param marker the marker
     * @param msg    the msg
     */
    default void logError(Marker marker, String msg) {
        this.logger().error(marker, msg);
    }

    /**
     * Log error.
     *
     * @param marker the marker
     * @param format the format
     * @param arg    the arg
     */
    default void logError(Marker marker, String format, Object arg) {
        this.logger().error(marker, format, arg);
    }

    /**
     * Log error.
     *
     * @param marker the marker
     * @param format the format
     * @param arg1   the arg 1
     * @param arg2   the arg 2
     */
    default void logError(Marker marker, String format, Object arg1, Object arg2) {
        this.logger().error(marker, format, arg1, arg2);
    }

    /**
     * Log error.
     *
     * @param marker    the marker
     * @param format    the format
     * @param arguments the arguments
     */
    default void logError(Marker marker, String format, Object... arguments) {
        this.logger().error(marker, format, arguments);
    }

    /**
     * Log error.
     *
     * @param marker the marker
     * @param msg    the msg
     * @param t      the t
     */
    default void logError(Marker marker, String msg, Throwable t) {
        this.logger().error(marker, msg, t);
    }
}
