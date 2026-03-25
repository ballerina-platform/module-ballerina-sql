/*
 * Copyright (c) 2025, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.sql.observability;

import com.zaxxer.hikari.metrics.PoolStats;
import io.ballerina.runtime.observability.metrics.Counter;
import io.ballerina.runtime.observability.metrics.DefaultMetricRegistry;
import io.ballerina.runtime.observability.metrics.Gauge;
import io.ballerina.runtime.observability.metrics.MetricRegistry;
import io.ballerina.runtime.observability.metrics.StatisticConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_CONN_ACQUISITION;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_CONN_CREATION;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_CONN_TIMEOUT;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_CONN_USAGE;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_POOL_ACTIVE;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_POOL_IDLE;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_POOL_INIT_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_POOL_MAX;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_POOL_MIN;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_POOL_PENDING;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_POOL_TOTAL;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.DESC_POOL_UTILIZATION;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_CONNECTION_ACQUISITION_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_CONNECTION_CREATION_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_CONNECTION_TIMEOUT_TOTAL;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_CONNECTION_USAGE_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_POOL_ACTIVE_CONNECTIONS;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_POOL_IDLE_CONNECTIONS;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_POOL_INIT_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_POOL_MAX_CONNECTIONS;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_POOL_MIN_CONNECTIONS;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_POOL_PENDING_REQUESTS;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_POOL_TOTAL_CONNECTIONS;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.METRIC_POOL_UTILIZATION_RATIO;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.MILLIS_TO_SECONDS;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.NANOS_TO_SECONDS;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.TAG_POOL_NAME;

/**
 * Utility methods for SQL observability metric registration, reporting, and lifecycle management.
 * <p>
 * Handles three categories:
 * <ul>
 *     <li>Pool health gauges (registered via HikariCP MetricsTrackerFactory)</li>
 *     <li>Connection event timing (recorded via HikariCP IMetricsTracker callbacks)</li>
 *     <li>SQL operation timing and counting (recorded from processor classes)</li>
 * </ul>
 * <p>
 * All public methods are safe to call from any thread. Metric registration and reporting
 * failures are silently swallowed and never propagate exceptions.
 *
 * @since 1.18.0
 */
public final class ObservabilityUtils {

    private ObservabilityUtils() {
    }

    // Shared StatisticConfig for all summarized timing gauges
    private static final StatisticConfig STATISTIC_CONFIG = StatisticConfig.builder()
            .percentiles(0.5, 0.75, 0.95, 0.99)
            .expiry(Duration.ofMinutes(10))
            .buckets(5)
            .build();

    // Pool health Gauges (keyed by pool name, for cleanup)
    private static final ConcurrentHashMap<String, List<Gauge>> poolGaugeRegistry =
            new ConcurrentHashMap<>();

    // PoolStats objects stored for push-based gauge refresh
    private static final ConcurrentHashMap<String, PoolStats> poolStatsRegistry =
            new ConcurrentHashMap<>();

    // Init time Gauge per pool (set once, cleaned up with pool)
    private static final ConcurrentHashMap<String, Gauge> initTimeGauges =
            new ConcurrentHashMap<>();

    // Cached timing Gauges (connection events)
    private static final ConcurrentHashMap<String, Gauge> gaugeCache =
            new ConcurrentHashMap<>();

    // Cached Counters (timeout, query total, query error)
    private static final ConcurrentHashMap<String, Counter> counterCache =
            new ConcurrentHashMap<>();

    // ---- Pool name generation ----

    /**
     * Generate a pool name for metric tagging. Uses the user-configured name if provided
     * (after sanitization), otherwise generates a random UUID.
     *
     * @param userConfiguredPoolName the user-configured pool name, or null
     * @return a non-empty pool name suitable for use as a metric tag value
     */
    public static String generatePoolName(String userConfiguredPoolName) {
        if (userConfiguredPoolName != null) {
            userConfiguredPoolName = userConfiguredPoolName.trim();
        }
        if (userConfiguredPoolName != null
                && !userConfiguredPoolName.isEmpty()) {
            // Note: colons are replaced by this regex — critical for isForPool
            // delimiter safety (cache keys use ':' as segment separator)
            String sanitized = userConfiguredPoolName
                    .replaceAll("[^a-zA-Z0-9._-]", "-")
                    .replaceAll("-{2,}", "-")
                    .replaceAll("^-|-$", "");
            if (!sanitized.isEmpty()) {
                return sanitized;
            }
        }
        return UUID.randomUUID().toString();
    }

    // ---- Pool health metric registration and teardown ----

    /**
     * Register pool health Gauges and store the PoolStats reference for push-based refresh.
     * Called by {@link SqlMetricsTrackerFactory#create} when HikariCP starts a pool.
     * Values are updated on every connection event via {@link #refreshPoolState(String)}.
     *
     * @param poolStats the HikariCP pool statistics object
     * @param poolName  the pool name for metric tagging
     */
    public static void registerPoolMetrics(PoolStats poolStats,
                                           String poolName) {
        try {
            List<Gauge> gauges = new ArrayList<>();
            gauges.add(Gauge.builder(METRIC_POOL_ACTIVE_CONNECTIONS)
                    .description(DESC_POOL_ACTIVE)
                    .tag(TAG_POOL_NAME, poolName).register());
            gauges.add(Gauge.builder(METRIC_POOL_IDLE_CONNECTIONS)
                    .description(DESC_POOL_IDLE)
                    .tag(TAG_POOL_NAME, poolName).register());
            gauges.add(Gauge.builder(METRIC_POOL_TOTAL_CONNECTIONS)
                    .description(DESC_POOL_TOTAL)
                    .tag(TAG_POOL_NAME, poolName).register());
            gauges.add(Gauge.builder(METRIC_POOL_PENDING_REQUESTS)
                    .description(DESC_POOL_PENDING)
                    .tag(TAG_POOL_NAME, poolName).register());
            gauges.add(Gauge.builder(METRIC_POOL_MAX_CONNECTIONS)
                    .description(DESC_POOL_MAX)
                    .tag(TAG_POOL_NAME, poolName).register());
            gauges.add(Gauge.builder(METRIC_POOL_MIN_CONNECTIONS)
                    .description(DESC_POOL_MIN)
                    .tag(TAG_POOL_NAME, poolName).register());
            gauges.add(Gauge.builder(METRIC_POOL_UTILIZATION_RATIO)
                    .description(DESC_POOL_UTILIZATION)
                    .tag(TAG_POOL_NAME, poolName).register());

            poolGaugeRegistry.put(poolName, gauges);
            poolStatsRegistry.put(poolName, poolStats);
            refreshPoolState(poolName);
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    /**
     * Refresh pool health gauge values from the stored PoolStats.
     * Called from MetricsTracker callbacks on every connection event.
     *
     * @param poolName the pool name whose gauges should be refreshed
     */
    static void refreshPoolState(String poolName) {
        try {
            List<Gauge> gauges = poolGaugeRegistry.get(poolName);
            PoolStats stats = poolStatsRegistry.get(poolName);
            if (gauges == null || stats == null || gauges.size() < 7) {
                return;
            }
            gauges.get(0).setValue(stats.getActiveConnections());
            gauges.get(1).setValue(stats.getIdleConnections());
            gauges.get(2).setValue(stats.getTotalConnections());
            gauges.get(3).setValue(stats.getPendingThreads());
            gauges.get(4).setValue(stats.getMaxConnections());
            gauges.get(5).setValue(stats.getMinConnections());
            int max = stats.getMaxConnections();
            gauges.get(6).setValue(max > 0
                    ? (double) stats.getActiveConnections() / max : 0.0);
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    /**
     * Record the time taken to initialize a connection pool.
     * Creates a Gauge set once after pool creation.
     *
     * @param poolName the pool name for metric tagging
     * @param seconds  initialization time in seconds
     */
    public static void recordPoolInitTime(String poolName, double seconds) {
        try {
            Gauge gauge = Gauge.builder(METRIC_POOL_INIT_TIME)
                    .description(DESC_POOL_INIT_TIME)
                    .tag(TAG_POOL_NAME, poolName)
                    .register();
            gauge.setValue(seconds);
            initTimeGauges.put(poolName, gauge);
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    /**
     * Unregister all metrics associated with a pool. Idempotent — silently returns
     * if the pool has no registered metrics. Removes pool health Gauges,
     * init time Gauge, and any cached SQL operation Gauges/Counters for the pool.
     *
     * @param poolName the pool name whose metrics should be cleaned up
     */
    public static void unregisterPoolMetrics(String poolName) {
        try {
            if (poolName == null || poolName.isEmpty()) {
                return;
            }
            MetricRegistry registry = DefaultMetricRegistry.getInstance();

            // Pool health Gauges
            List<Gauge> poolGauges = poolGaugeRegistry.remove(poolName);
            if (poolGauges != null) {
                for (Gauge gauge : poolGauges) {
                    registry.unregister(gauge);
                }
            }
            poolStatsRegistry.remove(poolName);

            // Init time Gauge
            Gauge initGauge = initTimeGauges.remove(poolName);
            if (initGauge != null) {
                registry.unregister(initGauge);
            }

            // Cached Gauges matching this pool
            gaugeCache.entrySet().removeIf(entry -> {
                if (isForPool(entry.getKey(), poolName)) {
                    registry.unregister(entry.getValue());
                    return true;
                }
                return false;
            });

            // Cached Counters matching this pool
            counterCache.entrySet().removeIf(entry -> {
                if (isForPool(entry.getKey(), poolName)) {
                    registry.unregister(entry.getValue());
                    return true;
                }
                return false;
            });
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    // ---- Connection event recording (called from SqlMetricsTracker) ----

    /**
     * Record connection acquisition wait time.
     *
     * @param poolName           pool name for metric tagging
     * @param elapsedAcquiredNanos time in nanoseconds
     */
    static void recordConnectionAcquisitionTime(String poolName,
                                                long elapsedAcquiredNanos) {
        try {
            String key = METRIC_CONNECTION_ACQUISITION_TIME + ":" + poolName;
            gaugeCache.computeIfAbsent(key, k ->
                    Gauge.builder(METRIC_CONNECTION_ACQUISITION_TIME)
                            .description(DESC_CONN_ACQUISITION)
                            .tag(TAG_POOL_NAME, poolName)
                            .summarize(STATISTIC_CONFIG)
                            .register()
            ).setValue(elapsedAcquiredNanos / NANOS_TO_SECONDS);
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    /**
     * Record connection usage (hold) duration.
     *
     * @param poolName            pool name for metric tagging
     * @param elapsedBorrowedMillis time in milliseconds
     */
    static void recordConnectionUsageTime(String poolName,
                                          long elapsedBorrowedMillis) {
        try {
            String key = METRIC_CONNECTION_USAGE_TIME + ":" + poolName;
            gaugeCache.computeIfAbsent(key, k ->
                    Gauge.builder(METRIC_CONNECTION_USAGE_TIME)
                            .description(DESC_CONN_USAGE)
                            .tag(TAG_POOL_NAME, poolName)
                            .summarize(STATISTIC_CONFIG)
                            .register()
            ).setValue(elapsedBorrowedMillis / MILLIS_TO_SECONDS);
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    /**
     * Record physical connection creation time.
     *
     * @param poolName               pool name for metric tagging
     * @param connectionCreatedMillis time in milliseconds
     */
    static void recordConnectionCreationTime(String poolName,
                                             long connectionCreatedMillis) {
        try {
            String key = METRIC_CONNECTION_CREATION_TIME + ":" + poolName;
            gaugeCache.computeIfAbsent(key, k ->
                    Gauge.builder(METRIC_CONNECTION_CREATION_TIME)
                            .description(DESC_CONN_CREATION)
                            .tag(TAG_POOL_NAME, poolName)
                            .summarize(STATISTIC_CONFIG)
                            .register()
            ).setValue(connectionCreatedMillis / MILLIS_TO_SECONDS);
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    /**
     * Increment the connection timeout counter.
     *
     * @param poolName pool name for metric tagging
     */
    static void recordConnectionTimeout(String poolName) {
        try {
            String key = METRIC_CONNECTION_TIMEOUT_TOTAL + ":" + poolName;
            counterCache.computeIfAbsent(key, k ->
                    Counter.builder(METRIC_CONNECTION_TIMEOUT_TOTAL)
                            .description(DESC_CONN_TIMEOUT)
                            .tag(TAG_POOL_NAME, poolName)
                            .register()
            ).increment();
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    // ---- Package-private test helpers ----

    /**
     * Check whether a pool has registered health metrics.
     * Package-private — used by tests to verify unregister guards.
     *
     * @param poolName the pool name to check
     * @return true if pool health gauges are registered for this name
     */
    static boolean hasRegisteredMetrics(String poolName) {
        return poolGaugeRegistry.containsKey(poolName);
    }

    // ---- Private helpers ----

    /**
     * Check if a cache key belongs to a given pool.
     * Keys follow the format "metricName:poolName[:operation[:errorClass]]".
     * UUID names contain only hex and dashes. User names are sanitized to
     * {@code [a-zA-Z0-9._-]}. Neither contains ':', so colon-delimited
     * matching is safe.
     */
    private static boolean isForPool(String key, String poolName) {
        return key.contains(":" + poolName + ":")
                || key.endsWith(":" + poolName);
    }
}
