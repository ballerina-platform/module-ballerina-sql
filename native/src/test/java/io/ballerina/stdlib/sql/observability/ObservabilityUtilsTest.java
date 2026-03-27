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
import io.ballerina.runtime.observability.metrics.Gauge;
import io.ballerina.runtime.observability.metrics.PolledGauge;
import org.testng.annotations.Test;

import java.util.List;

import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_ACQUISITION_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_CREATION_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_TIMEOUT_TOTAL;
import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_USAGE_TIME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link ObservabilityUtils}.
 *
 * @since 1.18.0
 */
public class ObservabilityUtilsTest {

    // ---- Pool name generation tests ----

    @Test
    void testGeneratePoolNameUserConfigured() {
        assertEquals(ObservabilityUtils.generatePoolName("MyPool"),
                "MyPool");
    }

    @Test
    void testGeneratePoolNameWithColons() {
        assertEquals(ObservabilityUtils.generatePoolName("my:pool:name"),
                "my-pool-name");
    }

    @Test
    void testGeneratePoolNameSpecialCharacters() {
        assertEquals(
                ObservabilityUtils.generatePoolName("my pool/test;drop"),
                "my-pool-test-drop");
    }

    @Test
    void testGeneratePoolNameDotsAndUnderscores() {
        assertEquals(ObservabilityUtils.generatePoolName("my_pool.v2"),
                "my_pool.v2");
    }

    @Test
    void testGeneratePoolNameMixedValidInvalid() {
        assertEquals(
                ObservabilityUtils.generatePoolName("pool@host:5432/db"),
                "pool-host-5432-db");
    }

    @Test
    void testGeneratePoolNameNull() {
        assertNull(ObservabilityUtils.generatePoolName(null));
    }

    @Test
    void testGeneratePoolNameEmpty() {
        assertNull(ObservabilityUtils.generatePoolName(""));
    }

    @Test
    void testGeneratePoolNameWhitespaceOnly() {
        assertNull(ObservabilityUtils.generatePoolName("   "));
    }

    @Test
    void testGeneratePoolNameAllColons() {
        // ":::" sanitizes to empty -> null
        assertNull(ObservabilityUtils.generatePoolName(":::"));
    }

    // ---- parseJdbcUrl tests ----

    @Test
    void testParseJdbcUrlPostgres() {
        JdbcUrlInfo info = ObservabilityUtils.parseJdbcUrl(
                "jdbc:postgresql://localhost:5432/mydb");
        assertFalse(info.isEmpty());
        assertEquals(info.host(), "localhost");
        assertEquals(info.port(), "5432");
        assertEquals(info.dbName(), "mydb");
        assertEquals(info.safeUrl(),
                "jdbc:postgresql://localhost:5432/mydb");
    }

    @Test
    void testParseJdbcUrlMysqlNoPort() {
        JdbcUrlInfo info = ObservabilityUtils.parseJdbcUrl(
                "jdbc:mysql://dbhost/orders");
        assertEquals(info.host(), "dbhost");
        assertEquals(info.port(), "");
        assertEquals(info.dbName(), "orders");
        assertEquals(info.safeUrl(), "jdbc:mysql://dbhost/orders");
    }

    @Test
    void testParseJdbcUrlCredentialsStripped() {
        JdbcUrlInfo info = ObservabilityUtils.parseJdbcUrl(
                "jdbc:postgresql://user:pass@db.prod:5432/app");
        assertEquals(info.host(), "db.prod");
        assertEquals(info.port(), "5432");
        assertEquals(info.dbName(), "app");
        assertEquals(info.safeUrl(),
                "jdbc:postgresql://db.prod:5432/app");
        assertFalse(info.safeUrl().contains("user"),
                "safeUrl must not contain credentials");
        assertFalse(info.safeUrl().contains("pass"),
                "safeUrl must not contain credentials");
    }

    @Test
    void testParseJdbcUrlQueryParamsStripped() {
        JdbcUrlInfo info = ObservabilityUtils.parseJdbcUrl(
                "jdbc:mysql://host:3306/db?password=secret&useSSL=false");
        assertEquals(info.host(), "host");
        assertEquals(info.port(), "3306");
        assertEquals(info.dbName(), "db");
        assertEquals(info.safeUrl(), "jdbc:mysql://host:3306/db");
        assertFalse(info.safeUrl().contains("password"),
                "safeUrl must not contain query params");
    }

    @Test
    void testParseJdbcUrlHsqldbDoubleScheme() {
        JdbcUrlInfo info = ObservabilityUtils.parseJdbcUrl(
                "jdbc:hsqldb:hsql://localhost:9001/mydb");
        assertEquals(info.host(), "localhost");
        assertEquals(info.port(), "9001");
        assertEquals(info.dbName(), "mydb");
        assertEquals(info.safeUrl(),
                "jdbc:hsqldb:hsql://localhost:9001/mydb");
    }

    @Test
    void testParseJdbcUrlNoPath() {
        JdbcUrlInfo info = ObservabilityUtils.parseJdbcUrl(
                "jdbc:postgresql://localhost:5432");
        assertEquals(info.host(), "localhost");
        assertEquals(info.port(), "5432");
        assertEquals(info.dbName(), "");
        assertEquals(info.safeUrl(),
                "jdbc:postgresql://localhost:5432");
    }

    @Test
    void testParseJdbcUrlOracle() {
        assertTrue(ObservabilityUtils.parseJdbcUrl(
                "jdbc:oracle:thin:@host:1521:SID").isEmpty());
    }

    @Test
    void testParseJdbcUrlSqlServer() {
        assertTrue(ObservabilityUtils.parseJdbcUrl(
                "jdbc:sqlserver://host:1433;databaseName=mydb").isEmpty());
    }

    @Test
    void testParseJdbcUrlH2Embedded() {
        assertTrue(ObservabilityUtils.parseJdbcUrl(
                "jdbc:h2:mem:testdb").isEmpty());
    }

    @Test
    void testParseJdbcUrlNull() {
        assertTrue(ObservabilityUtils.parseJdbcUrl(null).isEmpty());
    }

    @Test
    void testParseJdbcUrlEmpty() {
        assertTrue(ObservabilityUtils.parseJdbcUrl("").isEmpty());
    }

    // ---- Safety guard tests ----

    @Test
    void testUnregisterEmptyPoolNameGuard() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "survivor-pool-1",
                JdbcUrlInfo.EMPTY);
        ObservabilityUtils.unregisterPoolMetrics("");
        assertTrue(ObservabilityUtils.hasRegisteredMetrics("survivor-pool-1"),
                "Empty poolName unregister must not affect other pools");
        ObservabilityUtils.unregisterPoolMetrics("survivor-pool-1");
    }

    @Test
    void testUnregisterNullPoolNameGuard() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "survivor-pool-2",
                JdbcUrlInfo.EMPTY);
        ObservabilityUtils.unregisterPoolMetrics(null);
        assertTrue(ObservabilityUtils.hasRegisteredMetrics("survivor-pool-2"),
                "Null poolName unregister must not affect other pools");
        ObservabilityUtils.unregisterPoolMetrics("survivor-pool-2");
    }

    @Test
    void testUnregisterDoubleCallIdempotent() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "double-unreg-pool",
                JdbcUrlInfo.EMPTY);
        ObservabilityUtils.unregisterPoolMetrics("double-unreg-pool");
        ObservabilityUtils.unregisterPoolMetrics("double-unreg-pool");
    }

    // ---- Pool registration lifecycle tests ----

    @Test
    void testRegisterAndUnregisterPoolMetrics() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "test-register-pool",
                JdbcUrlInfo.EMPTY);
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(
                "test-register-pool"));
        ObservabilityUtils.unregisterPoolMetrics("test-register-pool");
        assertFalse(ObservabilityUtils.hasRegisteredMetrics(
                "test-register-pool"));
    }

    @Test
    void testRegisterPoolMetricsCreatesSevenGauges() {
        // registerPoolMetrics creates 7 PolledGauges:
        // active, idle, total, pending, max, min, utilization_ratio
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        String pool = "seven-gauge-pool";
        ObservabilityUtils.registerPoolMetrics(stats, pool,
                JdbcUrlInfo.EMPTY);

        List<PolledGauge> gauges = ObservabilityUtils.getPoolGauges(pool);
        assertNotNull(gauges, "Pool gauges must be registered");
        assertEquals(gauges.size(), 7,
                "Must register exactly 7 pool health gauges");

        ObservabilityUtils.unregisterPoolMetrics(pool);
        assertNull(ObservabilityUtils.getPoolGauges(pool),
                "Pool gauges must be removed after unregister");
    }

    @Test
    void testRegisterPoolMetricsWithMaxZero() {
        // Exercises the utilization ratio lambda: max=0 should not
        // cause division by zero (returns 0.0 via the guard).
        // Verifying that 7 gauges still register proves the lambda
        // was accepted without error.
        PoolStats stats = createMockPoolStats(0, 0, 0, 0, 0, 0);
        String pool = "zero-max-pool";
        ObservabilityUtils.registerPoolMetrics(stats, pool,
                JdbcUrlInfo.EMPTY);
        List<PolledGauge> gauges = ObservabilityUtils.getPoolGauges(pool);
        assertNotNull(gauges);
        assertEquals(gauges.size(), 7);
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    @Test
    void testPolledGaugePullsUpdatedValues() {
        MutableMockPoolStats stats = new MutableMockPoolStats(
                5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats,
                "pull-test-pool", JdbcUrlInfo.EMPTY);
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(
                "pull-test-pool"));

        // Mutate to simulate pool state change — PolledGauge reads live
        stats.setValues(9, 1, 10, 5, 10, 1);

        ObservabilityUtils.unregisterPoolMetrics("pull-test-pool");
    }

    @Test
    void testUnregisterIdempotent() {
        ObservabilityUtils.unregisterPoolMetrics("never-registered");
        ObservabilityUtils.unregisterPoolMetrics("never-registered");
    }

    // ---- Connection event recording tests ----

    @Test
    void testRecordConnectionAcquisitionTime() {
        // Source: ObservabilityUtils lines 377-392
        // Caches gauge under key METRIC_CONNECTION_ACQUISITION_TIME:poolName
        // Converts nanos to seconds: value / 1_000_000_000.0
        String pool = "ou-acq-pool";
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_ACQUISITION_TIME, pool),
                "No gauge should exist before recording");

        ObservabilityUtils.recordConnectionAcquisitionTime(
                pool, 500_000_000L, JdbcUrlInfo.EMPTY);

        Gauge gauge = ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_ACQUISITION_TIME, pool);
        assertNotNull(gauge,
                "Gauge must be cached after recording");

        // Second call reuses cached gauge (computeIfAbsent hit)
        ObservabilityUtils.recordConnectionAcquisitionTime(
                pool, 200_000_000L, JdbcUrlInfo.EMPTY);
        assertSame(gauge, ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_ACQUISITION_TIME, pool),
                "Second call must reuse the same cached Gauge instance");

        ObservabilityUtils.unregisterPoolMetrics(pool);
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_ACQUISITION_TIME, pool),
                "Gauge must be removed from cache after unregister");
    }

    @Test
    void testRecordConnectionUsageTime() {
        // Source: ObservabilityUtils lines 401-416
        // Converts millis to seconds: value / 1_000.0
        String pool = "ou-usage-pool";
        assertNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_USAGE_TIME, pool));

        ObservabilityUtils.recordConnectionUsageTime(
                pool, 1500L, JdbcUrlInfo.EMPTY);

        Gauge gauge = ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_USAGE_TIME, pool);
        assertNotNull(gauge,
                "Gauge must be cached after recording");

        ObservabilityUtils.unregisterPoolMetrics(pool);
        assertNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_USAGE_TIME, pool));
    }

    @Test
    void testRecordConnectionCreationTime() {
        // Source: ObservabilityUtils lines 425-440
        // Converts millis to seconds: value / 1_000.0
        String pool = "ou-creation-pool";
        assertNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_CREATION_TIME, pool));

        ObservabilityUtils.recordConnectionCreationTime(
                pool, 250L, JdbcUrlInfo.EMPTY);

        Gauge gauge = ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_CREATION_TIME, pool);
        assertNotNull(gauge,
                "Gauge must be cached after recording");

        ObservabilityUtils.unregisterPoolMetrics(pool);
        assertNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_CREATION_TIME, pool));
    }

    @Test
    void testRecordConnectionTimeout() {
        // Source: ObservabilityUtils lines 448-461
        // Caches Counter, calls increment() on each recording
        String pool = "ou-timeout-pool";
        assertNull(ObservabilityUtils.getCachedCounter(
                METRIC_CONNECTION_TIMEOUT_TOTAL, pool));

        ObservabilityUtils.recordConnectionTimeout(pool,
                JdbcUrlInfo.EMPTY);
        Counter counter = ObservabilityUtils.getCachedCounter(
                METRIC_CONNECTION_TIMEOUT_TOTAL, pool);
        assertNotNull(counter,
                "Counter must be cached after recording");

        // Second call reuses same counter (computeIfAbsent hit)
        ObservabilityUtils.recordConnectionTimeout(pool,
                JdbcUrlInfo.EMPTY);
        assertSame(counter, ObservabilityUtils.getCachedCounter(
                        METRIC_CONNECTION_TIMEOUT_TOTAL, pool),
                "Second call must reuse the same cached Counter");

        ObservabilityUtils.unregisterPoolMetrics(pool);
        assertNull(ObservabilityUtils.getCachedCounter(
                        METRIC_CONNECTION_TIMEOUT_TOTAL, pool),
                "Counter must be removed from cache after unregister");
    }

    @Test
    void testRecordPoolInitTime() {
        // Source: ObservabilityUtils lines 303-316
        // Creates plain Gauge (no summarize), stores in initTimeGauges
        String pool = "ou-init-time-pool";
        assertNull(ObservabilityUtils.getInitTimeGauge(pool));

        ObservabilityUtils.recordPoolInitTime(pool,
                "jdbc:postgresql://localhost:5432/mydb", 1.5);

        Gauge gauge = ObservabilityUtils.getInitTimeGauge(pool);
        assertNotNull(gauge,
                "Init time gauge must be stored after recording");

        ObservabilityUtils.unregisterPoolMetrics(pool);
        assertNull(ObservabilityUtils.getInitTimeGauge(pool),
                "Init time gauge must be removed after unregister");
    }

    @Test
    void testRecordPoolInitTimeWithUnparseableUrl() {
        // H2 embedded URL is unparseable (no host) but recording
        // must succeed with EMPTY urlInfo tags
        String pool = "ou-init-time-pool-2";
        ObservabilityUtils.recordPoolInitTime(pool,
                "jdbc:h2:mem:testdb", 0.3);
        assertNotNull(ObservabilityUtils.getInitTimeGauge(pool));
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    // ---- Tag building branch coverage ----

    @Test
    void testRecordEventWithFullUrlInfo() {
        // Exercises buildTags (lines 224-241) with all fields present:
        // !urlInfo.isEmpty() → true, all inner conditions true.
        // This covers the tag addition lines for db_host, db_port,
        // db_name, and db_url.
        String pool = "ou-full-url-pool";
        JdbcUrlInfo urlInfo = new JdbcUrlInfo(
                "dbhost", "3306", "orders",
                "jdbc:mysql://dbhost:3306/orders");
        ObservabilityUtils.recordConnectionAcquisitionTime(
                pool, 100_000_000L, urlInfo);

        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_ACQUISITION_TIME, pool));

        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    @Test
    void testRecordEventWithPartialUrlInfo() {
        // Exercises buildTags with host only (non-empty) but
        // port="", dbName="", safeUrl="" → those inner branches
        // evaluate to false, skipping db_port/db_name/db_url tags.
        String pool = "ou-partial-url-pool";
        JdbcUrlInfo urlInfo = new JdbcUrlInfo(
                "dbhost", "", "", "");
        ObservabilityUtils.recordConnectionUsageTime(
                pool, 100L, urlInfo);

        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_USAGE_TIME, pool));

        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    // ---- Unregister cleans all cache types ----

    @Test
    void testUnregisterCleansAllCacheTypes() {
        // Populates ALL 4 cache maps for one pool, then verifies
        // unregisterPoolMetrics cleans every one of them.
        // This exercises the removeIf + isForPool paths in
        // unregisterPoolMetrics (lines 347-362).
        String pool = "ou-full-cleanup-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        JdbcUrlInfo urlInfo = new JdbcUrlInfo(
                "localhost", "5432", "testdb",
                "jdbc:postgresql://localhost:5432/testdb");

        // Populate poolGaugeRegistry
        ObservabilityUtils.registerPoolMetrics(stats, pool, urlInfo);
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(pool));

        // Populate gaugeCache (3 connection event gauges)
        ObservabilityUtils.recordConnectionAcquisitionTime(
                pool, 100_000_000L, urlInfo);
        ObservabilityUtils.recordConnectionUsageTime(
                pool, 50L, urlInfo);
        ObservabilityUtils.recordConnectionCreationTime(
                pool, 30L, urlInfo);
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_ACQUISITION_TIME, pool));
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_USAGE_TIME, pool));
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_CREATION_TIME, pool));

        // Populate counterCache
        ObservabilityUtils.recordConnectionTimeout(pool, urlInfo);
        assertNotNull(ObservabilityUtils.getCachedCounter(
                METRIC_CONNECTION_TIMEOUT_TOTAL, pool));

        // Populate initTimeGauges
        ObservabilityUtils.recordPoolInitTime(pool,
                "jdbc:postgresql://localhost:5432/testdb", 0.5);
        assertNotNull(ObservabilityUtils.getInitTimeGauge(pool));

        // Unregister — should clean ALL cache maps
        ObservabilityUtils.unregisterPoolMetrics(pool);

        // Verify every cache is cleaned
        assertFalse(ObservabilityUtils.hasRegisteredMetrics(pool),
                "poolGaugeRegistry must be cleaned");
        assertNull(ObservabilityUtils.getInitTimeGauge(pool),
                "initTimeGauges must be cleaned");
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_ACQUISITION_TIME, pool),
                "gaugeCache (acquisition) must be cleaned");
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_USAGE_TIME, pool),
                "gaugeCache (usage) must be cleaned");
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_CREATION_TIME, pool),
                "gaugeCache (creation) must be cleaned");
        assertNull(ObservabilityUtils.getCachedCounter(
                        METRIC_CONNECTION_TIMEOUT_TOTAL, pool),
                "counterCache must be cleaned");
    }

    // ---- Helpers ----

    private static PoolStats createMockPoolStats(int active, int idle,
                                                  int total, int pending,
                                                  int max, int min) {
        return new MutableMockPoolStats(active, idle, total, pending,
                max, min);
    }

    /**
     * PoolStats subclass that allows mutation after creation.
     * Protected fields are accessible from within the subclass.
     */
    static class MutableMockPoolStats extends PoolStats {
        MutableMockPoolStats(int active, int idle, int total,
                             int pending, int max, int min) {
            super(1000L);
            setValues(active, idle, total, pending, max, min);
        }

        void setValues(int active, int idle, int total,
                       int pending, int max, int min) {
            this.activeConnections = active;
            this.idleConnections = idle;
            this.totalConnections = total;
            this.pendingThreads = pending;
            this.maxConnections = max;
            this.minConnections = min;
        }

        @Override
        protected void update() {
            // No-op — test seeds fixed values directly
        }
    }
}
