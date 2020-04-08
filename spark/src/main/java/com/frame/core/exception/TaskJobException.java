package com.frame.core.exception;

/**
 * @author XiaShuai on 2020/4/8.
 */
public class TaskJobException extends JobException {
    private static final long serialVersionUID = 3532142885796554437L;

    public TaskJobException() {
        super();
    }

    public TaskJobException(String message) {
        super(message);
    }

    public TaskJobException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskJobException(Throwable cause) {
        super(cause);
    }

    protected TaskJobException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
