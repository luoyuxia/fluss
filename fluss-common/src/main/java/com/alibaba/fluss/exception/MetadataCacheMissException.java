package com.alibaba.fluss.exception;

/** Metadata cache miss exception. */
public class MetadataCacheMissException extends RetriableException {

    public MetadataCacheMissException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetadataCacheMissException(String message) {
        super(message);
    }
}
