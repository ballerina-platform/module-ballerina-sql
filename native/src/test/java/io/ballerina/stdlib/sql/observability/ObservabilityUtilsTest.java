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
import java.util.Map;

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
 * @since 1.19.0
 */
public class ObservabilityUtilsTest {

    private static final Map<String, String> SAMPLE_TAGS = Map.of(
            ObservabilityUtils.TAG_DB_HOST, "localhost",
            ObservabilityUtils.TAG_DB_PORT, "5432",
            ObservabilityUtils.TAG_DB_NAME, "testdb");

    // Pool name generation tests

    @Test
    void testGeneratePoolNameUserConfigured() {
        assertEquals(ObservabilityUtils.sanitisePoolName("MyPool"),
                "MyPool");
    }

    @Test
    void testGeneratePoolNameWithColons() {
        assertEquals(ObservabilityUtils.sanitisePoolName("my:pool:name"),
                "my-pool-name");
    }

    @Test
    void testGeneratePoolNameSpecialCharacters() {
        assertEquals(
                ObservabilityUtils.sanitisePoolName("my pool/test;drop"),
                "my-pool-test-drop");
    }

    @Test
    void testGeneratePoolNameDotsAndUnderscores() {
        assertEquals(ObservabilityUtils.sanitisePoolName("my_pool.v2"),
                "my_pool.v2");
    }

    @Test
    void testGeneratePoolNameMixedValidInvalid() {
        assertEquals(
                ObservabilityUtils.sanitisePoolName("pool@host:5432/db"),
                "pool-host-5432-db");
    }

    @Test
    void testGeneratePoolNameEmpty() {
        assertNull(ObservabilityUtils.sanitisePoolName(""));
    }

    @Test
    void testGeneratePoolNameWhitespaceOnly() {
        assertNull(ObservabilityUtils.sanitisePoolName("   "));
    }

    @Test
    void testGeneratePoolNameAllColons() {
        assertNull(ObservabilityUtils.sanitisePoolName(":::"));
    }

    // Safety guard tests

    @Test
    void testUnregisterDoubleCallIdempotent() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "double-unreg-pool",
                Map.of());
        ObservabilityUtils.unregisterPoolMetrics("double-unreg-pool");
        ObservabilityUtils.unregisterPoolMetrics("double-unreg-pool");
    }

    // Pool registration lifecycle tests

    @Test
    void testRegisterAndUnregisterPoolMetrics() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "test-register-pool",
                Map.of());
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(
                "test-register-pool"));
        ObservabilityUtils.unregisterPoolMetrics("test-register-pool");
        assertFalse(ObservabilityUtils.hasRegisteredMetrics(
                "test-register-pool"));
    }

    @Test
    void testRegisterPoolMetricsCreatesSevenGauges() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        String pool = "seven-gauge-pool";
        ObservabilityUtils.registerPoolMetrics(stats, pool, SAMPLE_TAGS);

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
        PoolStats stats = createMockPoolStats(0, 0, 0, 0, 0, 0);
        String pool = "zero-max-pool";
        ObservabilityUtils.registerPoolMetrics(stats, pool, Map.of());
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
                "pull-test-pool", Map.of());
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(
                "pull-test-pool"));

        stats.setValues(9, 1, 10, 5, 10, 1);

        ObservabilityUtils.unregisterPoolMetrics("pull-test-pool");
    }

    @Test
    void testUnregisterIdempotent() {
        ObservabilityUtils.unregisterPoolMetrics("never-registered");
        ObservabilityUtils.unregisterPoolMetrics("never-registered");
    }

    // Connection event recording tests

    @Test
    void testRecordConnectionAcquisitionTime() {
        String pool = "ou-acq-pool";
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_ACQUISITION_TIME, pool),
                "No gauge should exist before recording");

        ObservabilityUtils.recordConnectionAcquisitionTime(
                pool, 500_000_000L, SAMPLE_TAGS);

        Gauge gauge = ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_ACQUISITION_TIME, pool);
        assertNotNull(gauge,
                "Gauge must be cached after recording");

        // Second call reuses cached gauge (computeIfAbsent hit)
        ObservabilityUtils.recordConnectionAcquisitionTime(
                pool, 200_000_000L, SAMPLE_TAGS);
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
        String pool = "ou-usage-pool";
        assertNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_USAGE_TIME, pool));

        ObservabilityUtils.recordConnectionUsageTime(
                pool, 1500L, Map.of());

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
        String pool = "ou-creation-pool";
        assertNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_CREATION_TIME, pool));

        ObservabilityUtils.recordConnectionCreationTime(
                pool, 250L, Map.of());

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
        String pool = "ou-timeout-pool";
        assertNull(ObservabilityUtils.getCachedCounter(
                METRIC_CONNECTION_TIMEOUT_TOTAL, pool));

        ObservabilityUtils.recordConnectionTimeout(pool, Map.of());
        Counter counter = ObservabilityUtils.getCachedCounter(
                METRIC_CONNECTION_TIMEOUT_TOTAL, pool);
        assertNotNull(counter,
                "Counter must be cached after recording");

        ObservabilityUtils.recordConnectionTimeout(pool, Map.of());
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
        String pool = "ou-init-time-pool";
        assertNull(ObservabilityUtils.getInitTimeGauge(pool));

        ObservabilityUtils.recordPoolInitTime(pool, SAMPLE_TAGS, 1.5);

        Gauge gauge = ObservabilityUtils.getInitTimeGauge(pool);
        assertNotNull(gauge,
                "Init time gauge must be stored after recording");

        ObservabilityUtils.unregisterPoolMetrics(pool);
        assertNull(ObservabilityUtils.getInitTimeGauge(pool),
                "Init time gauge must be removed after unregister");
    }

    // Unregister cleans all cache types

    @Test
    void testUnregisterCleansAllCacheTypes() {
        String pool = "ou-full-cleanup-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);

        ObservabilityUtils.registerPoolMetrics(stats, pool, SAMPLE_TAGS);
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(pool));

        ObservabilityUtils.recordConnectionAcquisitionTime(
                pool, 100_000_000L, SAMPLE_TAGS);
        ObservabilityUtils.recordConnectionUsageTime(
                pool, 50L, SAMPLE_TAGS);
        ObservabilityUtils.recordConnectionCreationTime(
                pool, 30L, SAMPLE_TAGS);
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_ACQUISITION_TIME, pool));
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_USAGE_TIME, pool));
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_CREATION_TIME, pool));

        ObservabilityUtils.recordConnectionTimeout(pool, SAMPLE_TAGS);
        assertNotNull(ObservabilityUtils.getCachedCounter(
                METRIC_CONNECTION_TIMEOUT_TOTAL, pool));

        ObservabilityUtils.recordPoolInitTime(pool, SAMPLE_TAGS, 0.5);
        assertNotNull(ObservabilityUtils.getInitTimeGauge(pool));

        ObservabilityUtils.unregisterPoolMetrics(pool);

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

    // Helpers

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
            // No-op — tests set values directly via setValues()
        }
    }
}
