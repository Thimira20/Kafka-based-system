package com.bigdata.orders;

/**
 * Signals a processing failure that will never succeed no matter how many
 * times it is retried (e.g. the order data itself is invalid). The consumer
 * sends these straight to the Dead Letter Queue without retrying.
 */
public class PermanentException extends Exception {
    public PermanentException(String message) {
        super(message);
    }
}
