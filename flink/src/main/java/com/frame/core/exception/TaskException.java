package com.frame.core.exception;

/**
 * @author XiaShuai on 2020/4/8.
 */
public class TaskException extends Exception {
    private static final long serialVersionUID = -4619459753579966922L;

    public TaskException() {
        super();
    }

    public TaskException(String message) {
        super(message);
    }

    public TaskException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskException(Throwable cause) {
        super(cause);
    }

    protected TaskException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
