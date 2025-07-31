package com.alibaba.fluss.exception;

/** Exception indicating that the maximum number of retries has been reached. */
public class RetryCountExceededException extends ApiException {

    private static final long serialVersionUID = 1L;

    public RetryCountExceededException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryCountExceededException(String message) {
        super(message);
    }

    public RetryCountExceededException(Throwable cause) {
        super(cause);
    }
}
