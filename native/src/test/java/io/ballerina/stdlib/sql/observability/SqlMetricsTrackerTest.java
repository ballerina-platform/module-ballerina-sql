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
import org.testng.annotations.Test;

import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_ACQUISITION_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_CREATION_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_TIMEOUT_TOTAL;
import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_USAGE_TIME;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link SqlMetricsTracker}.
 * Verifies that each IMetricsTracker method correctly delegates
 * to ObservabilityUtils and respects the closed state.
 *
 * @since 1.18.0
 */
public class SqlMetricsTrackerTest {

    // ---- Record methods verify delegation to ObservabilityUtils ----

    @Test
    void testRecordConnectionAcquiredNanos() {
        // Source: SqlMetricsTracker lines 44-50
        // When open: delegates to ObservabilityUtils.recordConnectionAcquisitionTime
        String pool = "smt-acq-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, JdbcUrlInfo.EMPTY);

        tracker.recordConnectionAcquiredNanos(500_000_000L);

        assertNotNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_ACQUISITION_TIME, pool),
                "Tracker must delegate to ObservabilityUtils, "
                        + "creating a cached gauge");
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    @Test
    void testRecordConnectionUsageMillis() {
        // Source: SqlMetricsTracker lines 53-59
        String pool = "smt-usage-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, JdbcUrlInfo.EMPTY);

        tracker.recordConnectionUsageMillis(150L);

        assertNotNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_USAGE_TIME, pool),
                "Tracker must delegate usage recording");
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    @Test
    void testRecordConnectionCreatedMillis() {
        // Source: SqlMetricsTracker lines 62-68
        String pool = "smt-create-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, JdbcUrlInfo.EMPTY);

        tracker.recordConnectionCreatedMillis(50L);

        assertNotNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_CREATION_TIME, pool),
                "Tracker must delegate creation recording");
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    @Test
    void testRecordConnectionTimeout() {
        // Source: SqlMetricsTracker lines 71-76
        String pool = "smt-timeout-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, JdbcUrlInfo.EMPTY);

        tracker.recordConnectionTimeout();

        assertNotNull(ObservabilityUtils.getCachedCounter(
                        METRIC_CONNECTION_TIMEOUT_TOTAL, pool),
                "Tracker must delegate timeout recording");
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    // ---- Record methods with URL info tags ----

    @Test
    void testRecordWithUrlInfoTags() {
        // Exercises all 4 record methods with non-empty urlInfo,
        // which triggers full buildTags path (db_host, db_port,
        // db_name, db_url tags all present).
        JdbcUrlInfo urlInfo = new JdbcUrlInfo(
                "dbhost", "5432", "mydb",
                "jdbc:postgresql://dbhost:5432/mydb");
        String pool = "smt-url-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(pool, urlInfo);

        tracker.recordConnectionAcquiredNanos(100_000_000L);
        tracker.recordConnectionUsageMillis(200L);
        tracker.recordConnectionCreatedMillis(30L);
        tracker.recordConnectionTimeout();

        // All 4 cache entries should exist
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_ACQUISITION_TIME, pool));
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_USAGE_TIME, pool));
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_CREATION_TIME, pool));
        assertNotNull(ObservabilityUtils.getCachedCounter(
                METRIC_CONNECTION_TIMEOUT_TOTAL, pool));

        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    // ---- Close lifecycle ----

    @Test
    void testCloseUnregistersPoolMetrics() {
        // Source: SqlMetricsTracker lines 79-86
        // close() sets closed=true then calls unregisterPoolMetrics
        String pool = "smt-close-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(
                stats, pool, JdbcUrlInfo.EMPTY);
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(pool));

        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, JdbcUrlInfo.EMPTY);
        tracker.close();
        assertFalse(ObservabilityUtils.hasRegisteredMetrics(pool),
                "close() must unregister pool metrics");
    }

    @Test
    void testCloseIsIdempotent() {
        String pool = "smt-close-idem-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(
                stats, pool, JdbcUrlInfo.EMPTY);

        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, JdbcUrlInfo.EMPTY);
        tracker.close();
        tracker.close(); // second close must not throw
        assertFalse(ObservabilityUtils.hasRegisteredMetrics(pool));
    }

    // ---- Record after close (no-op path) ----

    @Test
    void testRecordAfterCloseDoesNotCreateCacheEntries() {
        // Source: SqlMetricsTracker lines 45-46, 55-56, 65-66, 73-74
        // When closed=true, each record method returns immediately.
        // Verify that NO cache entries are created.
        String pool = "smt-closed-noop-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, JdbcUrlInfo.EMPTY);
        tracker.close();

        tracker.recordConnectionAcquiredNanos(100_000_000L);
        tracker.recordConnectionUsageMillis(50L);
        tracker.recordConnectionCreatedMillis(30L);
        tracker.recordConnectionTimeout();

        // All cache lookups must return null — closed tracker
        // did not delegate to ObservabilityUtils
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_ACQUISITION_TIME, pool),
                "Closed tracker must not create acquisition gauge");
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_USAGE_TIME, pool),
                "Closed tracker must not create usage gauge");
        assertNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_CREATION_TIME, pool),
                "Closed tracker must not create creation gauge");
        assertNull(ObservabilityUtils.getCachedCounter(
                        METRIC_CONNECTION_TIMEOUT_TOTAL, pool),
                "Closed tracker must not create timeout counter");
    }

    // ---- Helpers ----

    private static PoolStats createMockPoolStats(int active, int idle,
                                                  int total, int pending,
                                                  int max, int min) {
        return new ObservabilityUtilsTest.MutableMockPoolStats(
                active, idle, total, pending, max, min);
    }
}
