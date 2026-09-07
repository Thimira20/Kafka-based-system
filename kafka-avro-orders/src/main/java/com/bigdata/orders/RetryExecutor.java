package com.bigdata.orders;

import com.bigdata.orders.avro.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Retries a {@link TransientException} up to a fixed number of attempts,
 * using exponential backoff with jitter between attempts.
 *
 * <p>A {@link PermanentException} is never retried — it propagates
 * immediately so the caller can route it straight to the DLQ.</p>
 *
 * <p>Backoff is kept short (hundreds of milliseconds) on purpose: this runs
 * inside the consumer's poll loop, and a long block here risks exceeding
 * {@code max.poll.interval.ms} and triggering a consumer group rebalance.
 * A production system with slower/longer retries would instead publish to a
 * separate delayed "retry" topic rather than blocking the poll loop.</p>
 */
public class RetryExecutor {

    private static final Logger log = LoggerFactory.getLogger(RetryExecutor.class);

    /** Exposed so callers (e.g. the DLQ publisher) can record how many attempts were made. */
    public static final int MAX_ATTEMPTS = 3;
    private static final long BASE_BACKOFF_MS = 200;

    /**
     * Runs {@code processor.process(order)}, retrying on
     * {@link TransientException} with exponential backoff. Throws once
     * either a {@link PermanentException} occurs, or the transient failure
     * persists past {@link #MAX_ATTEMPTS} attempts (retries exhausted).
     */
    public static void executeWithRetry(OrderProcessor processor, Order order)
            throws PermanentException, TransientException {

        int attempt = 1;
        while (true) {
            try {
                processor.process(order);
                return;
            } catch (TransientException e) {
                if (attempt >= MAX_ATTEMPTS) {
                    log.warn("Retries exhausted for orderId={} after {} attempts: {}",
                            order.getOrderId(), attempt, e.getMessage());
                    throw e;
                }

                long backoffMs = backoffWithJitter(attempt);
                log.info("Transient failure for orderId={} (attempt {}/{}): {} - retrying in {}ms",
                        order.getOrderId(), attempt, MAX_ATTEMPTS, e.getMessage(), backoffMs);

                sleep(backoffMs);
                attempt++;
            }
        }
    }

    /** Exponential backoff (base * 2^(attempt-1)) with +/-20% jitter. */
    private static long backoffWithJitter(int attempt) {
        long base = BASE_BACKOFF_MS * (1L << (attempt - 1));
        double jitterFactor = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4; // 0.8x - 1.2x
        return Math.round(base * jitterFactor);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry backoff", e);
        }
    }
}
