package com.frame.core.exception;

/**
 * @author XiaShuai on 2020/4/8.
 */
public class JobException extends Exception {
    private static final long serialVersionUID = -6989971873857309910L;

    public JobException() {
        super();
    }

    public JobException(String message) {
        super(message);
    }

    public JobException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobException(Throwable cause) {
        super(cause);
    }

    protected JobException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
