/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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
import io.ballerina.runtime.observability.metrics.PolledGauge;
import io.ballerina.runtime.observability.metrics.StatisticConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    // Pool health PolledGauges (keyed by pool name, for cleanup)
    private static final ConcurrentHashMap<String, List<PolledGauge>> poolGaugeRegistry =
            new ConcurrentHashMap<>();

    // Init time Gauge per pool (set once, cleaned up with pool)
    private static final ConcurrentHashMap<String, Gauge> initTimeGauges =
            new ConcurrentHashMap<>();

    // Cached timing Gauges (connection events), keyed by poolName → metricName
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Gauge>> gaugeCache =
            new ConcurrentHashMap<>();

    // Cached Counters (timeout), keyed by poolName → metricName
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Counter>> counterCache =
            new ConcurrentHashMap<>();

    // Strong reference to PoolStats to prevent GC while PolledGauges are registered.
    // PolledGauge uses WeakReference for its state object; without this,
    // the PoolStats passed by HikariCP would be collected after factory creation.
    private static final ConcurrentHashMap<String, PoolStats> poolStatsRetainer =
            new ConcurrentHashMap<>();

    // Metric name constants (package-private for test access)

    static final String METRIC_POOL_ACTIVE_CONNECTIONS = "sql_pool_active_connections";
    static final String METRIC_POOL_IDLE_CONNECTIONS = "sql_pool_idle_connections";
    static final String METRIC_POOL_TOTAL_CONNECTIONS = "sql_pool_total_connections";
    static final String METRIC_POOL_PENDING_REQUESTS = "sql_pool_pending_requests";
    static final String METRIC_POOL_MAX_CONNECTIONS = "sql_pool_max_connections";
    static final String METRIC_POOL_MIN_CONNECTIONS = "sql_pool_min_connections";
    static final String METRIC_POOL_UTILIZATION_RATIO = "sql_pool_utilization_ratio";
    static final String METRIC_POOL_INIT_TIME = "sql_pool_initialization_time_seconds";
    static final String METRIC_CONNECTION_ACQUISITION_TIME = "sql_connection_acquisition_time_seconds";
    static final String METRIC_CONNECTION_USAGE_TIME = "sql_connection_usage_time_seconds";
    static final String METRIC_CONNECTION_CREATION_TIME = "sql_connection_creation_time_seconds";
    static final String METRIC_CONNECTION_TIMEOUT_TOTAL = "sql_connection_timeout_total";

    // Tag key constants (public for downstream module access)

    public static final String TAG_DB_HOST = "db_host";
    public static final String TAG_DB_PORT = "db_port";
    public static final String TAG_DB_NAME = "db_name";
    public static final String TAG_DB_URL = "db_url";

    // Private constants

    private static final String TAG_POOL_NAME = "pool_name";

    private static final String DESC_POOL_ACTIVE = "Connections currently borrowed from pool";
    private static final String DESC_POOL_IDLE = "Connections available in pool";
    private static final String DESC_POOL_TOTAL = "Active + idle (total pool size)";
    private static final String DESC_POOL_PENDING = "Application threads waiting for a connection";
    private static final String DESC_POOL_MAX = "Configured maximum pool size";
    private static final String DESC_POOL_MIN = "Configured minimum idle connections";
    private static final String DESC_POOL_UTILIZATION = "Active / max connection utilization ratio";
    private static final String DESC_POOL_INIT_TIME = "Time taken to initialize the connection pool in seconds";
    private static final String DESC_CONN_ACQUISITION = "Time a thread waited to get a connection from the pool";
    private static final String DESC_CONN_USAGE = "Time a connection was held before being returned";
    private static final String DESC_CONN_CREATION = "Time to create a new physical connection";
    private static final String DESC_CONN_TIMEOUT = "Connection acquisitions that timed out";

    public static final double NANOS_TO_SECONDS = 1_000_000_000.0;
    private static final double MILLIS_TO_SECONDS = 1_000.0;

    // Pool name generation

    /**
     * Generate a pool name for metric tagging. Uses the user-configured name
     * if provided (after sanitization), otherwise returns {@code null} to signal
     * that the caller should read back HikariCP's auto-generated name after
     * pool creation.
     *
     * @param userConfiguredPoolName the user-configured pool name, or null
     * @return a sanitized pool name, or null if naming must be deferred to HikariCP
     */
    public static String sanitisePoolName(String userConfiguredPoolName) {
        userConfiguredPoolName = userConfiguredPoolName.trim();
        if (userConfiguredPoolName.isEmpty()) {
            return null;
        }
        String sanitized = userConfiguredPoolName
                .replaceAll("[^a-zA-Z0-9._-]", "-")
                .replaceAll("-{2,}", "-")
                .replaceAll("(^-)|(-$)", "");
        return sanitized.isEmpty() ? null : sanitized;
    }

    // Tag helpers

    private static Map<String, String> buildTags(String poolName,
                                                 Map<String, String> metricsTags) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(TAG_POOL_NAME, poolName);
        tags.putAll(metricsTags);
        return tags;
    }

    // Pool health metric registration and teardown

    /**
     * Register pool health PolledGauges that read values on Prometheus scrape.
     * Called by {@link SqlMetricsTrackerFactory#create} when HikariCP starts a pool.
     * Values are pulled from the {@link PoolStats} object on each scrape — zero
     * overhead between scrapes.
     *
     * @param poolStats   the HikariCP pool statistics object
     * @param poolName    the pool name for metric tagging
     * @param metricsTags supplementary tags from the downstream database module, or null
     */
    public static void registerPoolMetrics(PoolStats poolStats,
                                           String poolName,
                                           Map<String, String> metricsTags) {
        try {
            Map<String, String> tags = buildTags(poolName, metricsTags);
            List<PolledGauge> gauges = new ArrayList<>();
            gauges.add(PolledGauge.builder(METRIC_POOL_ACTIVE_CONNECTIONS,
                    poolStats, PoolStats::getActiveConnections)
                    .description(DESC_POOL_ACTIVE).tags(tags).register());
            gauges.add(PolledGauge.builder(METRIC_POOL_IDLE_CONNECTIONS,
                    poolStats, PoolStats::getIdleConnections)
                    .description(DESC_POOL_IDLE).tags(tags).register());
            gauges.add(PolledGauge.builder(METRIC_POOL_TOTAL_CONNECTIONS,
                    poolStats, PoolStats::getTotalConnections)
                    .description(DESC_POOL_TOTAL).tags(tags).register());
            gauges.add(PolledGauge.builder(METRIC_POOL_PENDING_REQUESTS,
                    poolStats, PoolStats::getPendingThreads)
                    .description(DESC_POOL_PENDING).tags(tags).register());
            gauges.add(PolledGauge.builder(METRIC_POOL_MAX_CONNECTIONS,
                    poolStats, PoolStats::getMaxConnections)
                    .description(DESC_POOL_MAX).tags(tags).register());
            gauges.add(PolledGauge.builder(METRIC_POOL_MIN_CONNECTIONS,
                    poolStats, PoolStats::getMinConnections)
                    .description(DESC_POOL_MIN).tags(tags).register());
            gauges.add(PolledGauge.builder(METRIC_POOL_UTILIZATION_RATIO,
                    poolStats, stats -> {
                        int max = stats.getMaxConnections();
                        return max > 0
                                ? (double) stats.getActiveConnections()
                                / max : 0.0;
                    })
                    .description(DESC_POOL_UTILIZATION)
                    .tags(tags).register());

            poolGaugeRegistry.put(poolName, gauges);
            poolStatsRetainer.put(poolName, poolStats);
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    /**
     * Record the time taken to initialize a connection pool.
     * Creates a Gauge set once after pool creation.
     *
     * @param poolName    the pool name for metric tagging
     * @param metricsTags supplementary tags from the downstream database module, or null
     * @param seconds     initialization time in seconds
     */
    public static void recordPoolInitTime(String poolName,
                                          Map<String, String> metricsTags,
                                          double seconds) {
        try {
            Gauge gauge = Gauge.builder(METRIC_POOL_INIT_TIME)
                    .description(DESC_POOL_INIT_TIME)
                    .tags(buildTags(poolName, metricsTags))
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
            MetricRegistry registry = DefaultMetricRegistry.getInstance();

            // Pool health PolledGauges
            List<PolledGauge> poolGauges = poolGaugeRegistry.remove(poolName);
            poolStatsRetainer.remove(poolName);
            if (poolGauges != null) {
                for (PolledGauge gauge : poolGauges) {
                    registry.unregister(gauge);
                }
            }

            // Init time Gauge
            Gauge initGauge = initTimeGauges.remove(poolName);
            if (initGauge != null) {
                registry.unregister(initGauge);
            }

            // Cached Gauges for this pool
            ConcurrentHashMap<String, Gauge> cachedGauges =
                    gaugeCache.remove(poolName);
            if (cachedGauges != null) {
                cachedGauges.values().forEach(registry::unregister);
            }

            // Cached Counters for this pool
            ConcurrentHashMap<String, Counter> cachedCounters =
                    counterCache.remove(poolName);
            if (cachedCounters != null) {
                cachedCounters.values().forEach(registry::unregister);
            }
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    // Connection event recording (called from SqlMetricsTracker)

    /**
     * Record connection acquisition wait time.
     *
     * @param poolName             pool name for metric tagging
     * @param elapsedAcquiredNanos time in nanoseconds
     * @param metricsTags          supplementary tags, or null
     */
    static void recordConnectionAcquisitionTime(String poolName,
                                                long elapsedAcquiredNanos,
                                                Map<String, String> metricsTags) {
        try {
            gaugeCache.computeIfAbsent(poolName, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(METRIC_CONNECTION_ACQUISITION_TIME, k ->
                            Gauge.builder(METRIC_CONNECTION_ACQUISITION_TIME)
                                    .description(DESC_CONN_ACQUISITION)
                                    .tags(buildTags(poolName, metricsTags))
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
     * @param poolName              pool name for metric tagging
     * @param elapsedBorrowedMillis time in milliseconds
     * @param metricsTags           supplementary tags, or null
     */
    static void recordConnectionUsageTime(String poolName,
                                          long elapsedBorrowedMillis,
                                          Map<String, String> metricsTags) {
        try {
            gaugeCache.computeIfAbsent(poolName, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(METRIC_CONNECTION_USAGE_TIME, k ->
                            Gauge.builder(METRIC_CONNECTION_USAGE_TIME)
                                    .description(DESC_CONN_USAGE)
                                    .tags(buildTags(poolName, metricsTags))
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
     * @param metricsTags            supplementary tags, or null
     */
    static void recordConnectionCreationTime(String poolName,
                                             long connectionCreatedMillis,
                                             Map<String, String> metricsTags) {
        try {
            gaugeCache.computeIfAbsent(poolName, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(METRIC_CONNECTION_CREATION_TIME, k ->
                            Gauge.builder(METRIC_CONNECTION_CREATION_TIME)
                                    .description(DESC_CONN_CREATION)
                                    .tags(buildTags(poolName, metricsTags))
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
     * @param poolName    pool name for metric tagging
     * @param metricsTags supplementary tags, or null
     */
    static void recordConnectionTimeout(String poolName,
                                        Map<String, String> metricsTags) {
        try {
            counterCache.computeIfAbsent(poolName, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(METRIC_CONNECTION_TIMEOUT_TOTAL, k ->
                            Counter.builder(METRIC_CONNECTION_TIMEOUT_TOTAL)
                                    .description(DESC_CONN_TIMEOUT)
                                    .tags(buildTags(poolName, metricsTags))
                                    .register()
                    ).increment();
        } catch (Exception e) {
            // Silently swallow — observability failures must never affect pool operations
        }
    }

    // Package-private test helpers

    static boolean hasRegisteredMetrics(String poolName) {
        return poolGaugeRegistry.containsKey(poolName);
    }

    static List<PolledGauge> getPoolGauges(String poolName) {
        return poolGaugeRegistry.get(poolName);
    }

    static Gauge getCachedGauge(String metricName, String poolName) {
        ConcurrentHashMap<String, Gauge> inner = gaugeCache.get(poolName);
        return inner != null ? inner.get(metricName) : null;
    }

    static Counter getCachedCounter(String metricName, String poolName) {
        ConcurrentHashMap<String, Counter> inner = counterCache.get(poolName);
        return inner != null ? inner.get(metricName) : null;
    }

    static Gauge getInitTimeGauge(String poolName) {
        return initTimeGauges.get(poolName);
    }

}
