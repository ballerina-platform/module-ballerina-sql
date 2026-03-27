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
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.TAG_DB_HOST;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.TAG_DB_NAME;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.TAG_DB_PORT;
import static io.ballerina.stdlib.sql.observability.ObservabilityConstants.TAG_DB_URL;
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
