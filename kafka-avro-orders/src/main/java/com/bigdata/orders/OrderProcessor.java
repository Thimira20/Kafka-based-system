package com.bigdata.orders;

import com.bigdata.orders.avro.Order;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The "business logic" applied to each order. This is where an order is
 * validated and (in a real system) would be persisted, charged, shipped,
 * etc. For this assignment it deliberately fails in controlled, repeatable
 * ways so the retry and DLQ paths can be demonstrated live:
 *
 * <ul>
 *   <li>an invalid order (bad price / blank id / blank product) is a
 *       {@link PermanentException} — no amount of retrying will fix it,
 *       so it should go straight to the DLQ.</li>
 *   <li>a product name starting with "FLAKY" simulates a downstream
 *       dependency that is briefly unavailable: it throws
 *       {@link TransientException} on its first two attempts for a given
 *       order, then succeeds on the third — demonstrating retry recovery.</li>
 * </ul>
 */
public class OrderProcessor {

    /** How many times a given order has been attempted so far (for the FLAKY simulation only). */
    private final Map<String, Integer> attemptsByOrderId = new ConcurrentHashMap<>();

    /** Number of attempts a FLAKY order fails before it is allowed to succeed. */
    private static final int FLAKY_FAILS_BEFORE_SUCCESS = 2;

    public void process(Order order) throws PermanentException, TransientException {
        validatePermanent(order);

        int attempt = attemptsByOrderId.merge(order.getOrderId(), 1, Integer::sum);

        if (isFlaky(order) && attempt <= FLAKY_FAILS_BEFORE_SUCCESS) {
            throw new TransientException(
                    "Simulated downstream unavailability for orderId=" + order.getOrderId()
                            + " (attempt " + attempt + "/" + FLAKY_FAILS_BEFORE_SUCCESS + " expected to fail)");
        }

        // "Processing" succeeded — in a real system this is where the order
        // would be persisted / charged / shipped etc.
    }

    private void validatePermanent(Order order) throws PermanentException {
        if (order.getOrderId() == null || order.getOrderId().isBlank()) {
            throw new PermanentException("Order has a blank orderId");
        }
        if (order.getProduct() == null || order.getProduct().isBlank()) {
            throw new PermanentException("Order " + order.getOrderId() + " has a blank product");
        }
        if (order.getPrice() <= 0f) {
            throw new PermanentException(
                    "Order " + order.getOrderId() + " has an invalid price: " + order.getPrice());
        }
    }

    private boolean isFlaky(Order order) {
        return order.getProduct() != null && order.getProduct().startsWith("FLAKY");
    }
}
