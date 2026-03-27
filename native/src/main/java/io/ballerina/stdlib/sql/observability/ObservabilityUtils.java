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

import java.net.URI;
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

    // Cached timing Gauges (connection events)
    private static final ConcurrentHashMap<String, Gauge> gaugeCache =
            new ConcurrentHashMap<>();

    // Cached Counters (timeout, query total, query error)
    private static final ConcurrentHashMap<String, Counter> counterCache =
            new ConcurrentHashMap<>();

    // Strong reference to PoolStats to prevent GC while PolledGauges are registered.
    // PolledGauge uses WeakReference for its state object; without this,
    // the PoolStats passed by HikariCP would be collected after factory creation.
    private static final ConcurrentHashMap<String, PoolStats> poolStatsRetainer =
            new ConcurrentHashMap<>();

    // ---- Metric name constants (package-private for test access) ----

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

    // ---- Private constants ----

    private static final String TAG_POOL_NAME = "pool_name";
    private static final String TAG_DB_HOST = "db_host";
    private static final String TAG_DB_PORT = "db_port";
    private static final String TAG_DB_NAME = "db_name";
    private static final String TAG_DB_URL = "db_url";

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

    private static final double NANOS_TO_SECONDS = 1_000_000_000.0;
    private static final double MILLIS_TO_SECONDS = 1_000.0;

    // ---- Pool name generation ----

    /**
     * Generate a pool name for metric tagging. Uses the user-configured name
     * if provided (after sanitization), otherwise returns {@code null} to signal
     * that the caller should read back HikariCP's auto-generated name after
     * pool creation.
     *
     * @param userConfiguredPoolName the user-configured pool name, or null
     * @return a sanitized pool name, or null if naming must be deferred to HikariCP
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
                    .replaceAll("(^-)|(-$)", "");
            if (!sanitized.isEmpty()) {
                return sanitized;
            }
        }
        return null;
    }

    /**
     * Parse a JDBC URL into structured components for metric tagging.
     * Extracts host, port, database name, and reconstructs a safe URL
     * (no credentials, no query params). Handles double-scheme URLs
     * (e.g., {@code jdbc:hsqldb:hsql://host:port/db}).
     *
     * @param jdbcUrl the JDBC connection URL
     * @return parsed components, or {@link JdbcUrlInfo#EMPTY} if unparseable
     */
    static JdbcUrlInfo parseJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return JdbcUrlInfo.EMPTY;
        }
        try {
            // Strip "jdbc:" prefix
            String stripped = jdbcUrl.length() > 5
                    ? jdbcUrl.substring(5) : jdbcUrl;
            URI uri = new URI(stripped);
            String host = uri.getHost();

            // Double-scheme fallback for URLs like "hsqldb:hsql://localhost:9001/mydb".
            // After stripping "jdbc:", the remaining "hsqldb:hsql://..." is opaque.
            // Find "://" and re-parse from just before the "//" onward.
            if (host == null) {
                int schemeEnd = stripped.indexOf("://");
                if (schemeEnd > 0) {
                    uri = new URI(stripped.substring(schemeEnd + 1));
                    host = uri.getHost();
                }
            }

            if (host == null || host.isEmpty()) {
                return JdbcUrlInfo.EMPTY;
            }

            int port = uri.getPort();
            String portStr = port > 0 ? String.valueOf(port) : "";

            String path = uri.getPath();
            String db = (path != null && path.startsWith("/"))
                    ? path.substring(1) : "";

            String safeUrl = reconstructSafeUrl(jdbcUrl, host, port, db);

            return new JdbcUrlInfo(host, portStr, db, safeUrl);
        } catch (Exception e) {
            return JdbcUrlInfo.EMPTY;
        }
    }

    /**
     * Reconstruct a safe JDBC URL from parsed components. Uses the scheme
     * prefix from the original URL (everything before {@code ://}) and
     * appends only parsed host, port, and database — no credentials,
     * no query parameters.
     */
    private static String reconstructSafeUrl(String jdbcUrl, String host,
                                             int port, String db) {
        int schemeEnd = jdbcUrl.indexOf("://");
        if (schemeEnd < 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(
                jdbcUrl.substring(0, schemeEnd));
        sb.append("://").append(host);
        if (port > 0) {
            sb.append(":").append(port);
        }
        if (!db.isEmpty()) {
            sb.append("/").append(db);
        }
        return sb.toString();
    }


    // ---- Tag helpers ----

    /**
     * Build the tag map for a metric. Always includes {@code pool_name}.
     * Conditionally includes {@code db_host}, {@code db_port},
     * {@code db_name}, and {@code db_url} when URL parsing succeeded.
     */
    private static Map<String, String> buildTags(String poolName,
                                                 JdbcUrlInfo urlInfo) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(TAG_POOL_NAME, poolName);
        if (!urlInfo.isEmpty()) {
            tags.put(TAG_DB_HOST, urlInfo.host());
            if (!urlInfo.port().isEmpty()) {
                tags.put(TAG_DB_PORT, urlInfo.port());
            }
            if (!urlInfo.dbName().isEmpty()) {
                tags.put(TAG_DB_NAME, urlInfo.dbName());
            }
            if (!urlInfo.safeUrl().isEmpty()) {
                tags.put(TAG_DB_URL, urlInfo.safeUrl());
            }
        }
        return tags;
    }

    // ---- Pool health metric registration and teardown ----

    /**
     * Register pool health PolledGauges that read values on Prometheus scrape.
     * Called by {@link SqlMetricsTrackerFactory#create} when HikariCP starts a pool.
     * Values are pulled from the {@link PoolStats} object on each scrape — zero
     * overhead between scrapes.
     *
     * @param poolStats the HikariCP pool statistics object
     * @param poolName  the pool name for metric tagging
     * @param urlInfo   parsed JDBC URL components for supplementary tags
     */
    public static void registerPoolMetrics(PoolStats poolStats,
                                           String poolName,
                                           JdbcUrlInfo urlInfo) {
        try {
            Map<String, String> tags = buildTags(poolName, urlInfo);
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
     * @param poolName the pool name for metric tagging
     * @param jdbcUrl  the JDBC URL for supplementary tags
     * @param seconds  initialization time in seconds
     */
    public static void recordPoolInitTime(String poolName, String jdbcUrl,
                                          double seconds) {
        try {
            JdbcUrlInfo urlInfo = parseJdbcUrl(jdbcUrl);
            Gauge gauge = Gauge.builder(METRIC_POOL_INIT_TIME)
                    .description(DESC_POOL_INIT_TIME)
                    .tags(buildTags(poolName, urlInfo))
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
     * @param urlInfo            parsed JDBC URL components for supplementary tags
     */
    static void recordConnectionAcquisitionTime(String poolName,
                                                long elapsedAcquiredNanos,
                                                JdbcUrlInfo urlInfo) {
        try {
            String key = METRIC_CONNECTION_ACQUISITION_TIME + ":" + poolName;
            gaugeCache.computeIfAbsent(key, k ->
                    Gauge.builder(METRIC_CONNECTION_ACQUISITION_TIME)
                            .description(DESC_CONN_ACQUISITION)
                            .tags(buildTags(poolName, urlInfo))
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
     * @param urlInfo             parsed JDBC URL components for supplementary tags
     */
    static void recordConnectionUsageTime(String poolName,
                                          long elapsedBorrowedMillis,
                                          JdbcUrlInfo urlInfo) {
        try {
            String key = METRIC_CONNECTION_USAGE_TIME + ":" + poolName;
            gaugeCache.computeIfAbsent(key, k ->
                    Gauge.builder(METRIC_CONNECTION_USAGE_TIME)
                            .description(DESC_CONN_USAGE)
                            .tags(buildTags(poolName, urlInfo))
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
     * @param urlInfo                parsed JDBC URL components for supplementary tags
     */
    static void recordConnectionCreationTime(String poolName,
                                             long connectionCreatedMillis,
                                             JdbcUrlInfo urlInfo) {
        try {
            String key = METRIC_CONNECTION_CREATION_TIME + ":" + poolName;
            gaugeCache.computeIfAbsent(key, k ->
                    Gauge.builder(METRIC_CONNECTION_CREATION_TIME)
                            .description(DESC_CONN_CREATION)
                            .tags(buildTags(poolName, urlInfo))
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
     * @param urlInfo  parsed JDBC URL components for supplementary tags
     */
    static void recordConnectionTimeout(String poolName,
                                        JdbcUrlInfo urlInfo) {
        try {
            String key = METRIC_CONNECTION_TIMEOUT_TOTAL + ":" + poolName;
            counterCache.computeIfAbsent(key, k ->
                    Counter.builder(METRIC_CONNECTION_TIMEOUT_TOTAL)
                            .description(DESC_CONN_TIMEOUT)
                            .tags(buildTags(poolName, urlInfo))
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

    /**
     * Return the list of pool health PolledGauges for a pool.
     * Package-private — used by tests to verify gauge count.
     */
    static List<PolledGauge> getPoolGauges(String poolName) {
        return poolGaugeRegistry.get(poolName);
    }

    /**
     * Return a cached connection event Gauge by metric name and pool name.
     * Package-private — used by tests to verify gauge creation and values.
     */
    static Gauge getCachedGauge(String metricName, String poolName) {
        return gaugeCache.get(metricName + ":" + poolName);
    }

    /**
     * Return a cached event Counter by metric name and pool name.
     * Package-private — used by tests to verify counter creation.
     */
    static Counter getCachedCounter(String metricName, String poolName) {
        return counterCache.get(metricName + ":" + poolName);
    }

    /**
     * Return the init time Gauge for a pool.
     * Package-private — used by tests to verify init time recording.
     */
    static Gauge getInitTimeGauge(String poolName) {
        return initTimeGauges.get(poolName);
    }

    // ---- Private helpers ----

    /**
     * Check if a cache key belongs to a given pool.
     * Keys follow the format "metricName:poolName[:operation[:errorClass]]".
     * URL-derived names go through the same sanitization regex as user names,
     * and HikariCP auto-generated names (e.g., "HikariPool-1") use only
     * alphanumeric characters and hyphens. None of these contain ':', so
     * colon-delimited matching is safe.
     */
    private static boolean isForPool(String key, String poolName) {
        return key.contains(":" + poolName + ":")
                || key.endsWith(":" + poolName);
    }
}
