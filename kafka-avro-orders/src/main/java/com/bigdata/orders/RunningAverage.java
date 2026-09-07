package com.bigdata.orders;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks a running (incremental) average of order prices, both globally and
 * per product, without storing the individual values.
 *
 * <p>Only successfully processed orders should be fed in here — a value that
 * was sent to the DLQ must never affect the average.</p>
 *
 * <p>The running sum is kept as a {@code double} even though prices are
 * {@code float}, to avoid accumulating floating-point drift over many
 * additions.</p>
 */
public class RunningAverage {

    /** Per-key (e.g. per product) running stats. */
    private static class Stats {
        long count = 0;
        double sum = 0.0;

        synchronized void add(float value) {
            count++;
            sum += value;
        }

        synchronized double average() {
            return count == 0 ? 0.0 : sum / count;
        }

        synchronized long count() {
            return count;
        }
    }

    private final Stats global = new Stats();
    private final Map<String, Stats> perProduct = new ConcurrentHashMap<>();

    /** Records a new price into both the global and per-product averages. */
    public void add(String product, float price) {
        global.add(price);
        perProduct.computeIfAbsent(product, p -> new Stats()).add(price);
    }

    public long globalCount() {
        return global.count();
    }

    public double globalAverage() {
        return global.average();
    }

    public double productAverage(String product) {
        Stats stats = perProduct.get(product);
        return stats == null ? 0.0 : stats.average();
    }

    public long productCount(String product) {
        Stats stats = perProduct.get(product);
        return stats == null ? 0 : stats.count();
    }

    /** A one-line summary of the global average and every product's average, for logging. */
    public String summary() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("processed=%d globalAvg=%.2f", globalCount(), globalAverage()));
        perProduct.forEach((product, stats) ->
                sb.append(String.format(" | %s avg=%.2f (n=%d)", product, stats.average(), stats.count())));
        return sb.toString();
    }
}
