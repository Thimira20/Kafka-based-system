package com.bigdata.orders;

/**
 * Signals a temporary processing failure that is expected to succeed if
 * retried (e.g. a downstream dependency that is briefly unavailable).
 * The consumer retries these with backoff before giving up.
 */
public class TransientException extends Exception {
    public TransientException(String message) {
        super(message);
    }
}
