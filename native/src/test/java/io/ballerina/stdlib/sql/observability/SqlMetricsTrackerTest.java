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

import java.util.Map;

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

    private static final Map<String, String> SAMPLE_TAGS = Map.of(
            ObservabilityUtils.TAG_DB_HOST, "localhost",
            ObservabilityUtils.TAG_DB_PORT, "5432",
            ObservabilityUtils.TAG_DB_NAME, "testdb");

    // Record methods verify delegation to ObservabilityUtils

    @Test
    void testRecordConnectionAcquiredNanos() {
        String pool = "smt-acq-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(pool, Map.of());

        tracker.recordConnectionAcquiredNanos(500_000_000L);

        assertNotNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_ACQUISITION_TIME, pool),
                "Tracker must delegate to ObservabilityUtils, "
                        + "creating a cached gauge");
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    @Test
    void testRecordConnectionUsageMillis() {
        String pool = "smt-usage-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(pool, Map.of());

        tracker.recordConnectionUsageMillis(150L);

        assertNotNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_USAGE_TIME, pool),
                "Tracker must delegate usage recording");
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    @Test
    void testRecordConnectionCreatedMillis() {
        String pool = "smt-create-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(pool, Map.of());

        tracker.recordConnectionCreatedMillis(50L);

        assertNotNull(ObservabilityUtils.getCachedGauge(
                        METRIC_CONNECTION_CREATION_TIME, pool),
                "Tracker must delegate creation recording");
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    @Test
    void testRecordConnectionTimeout() {
        String pool = "smt-timeout-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(pool, Map.of());

        tracker.recordConnectionTimeout();

        assertNotNull(ObservabilityUtils.getCachedCounter(
                        METRIC_CONNECTION_TIMEOUT_TOTAL, pool),
                "Tracker must delegate timeout recording");
        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    // Record methods with metric tags

    @Test
    void testRecordWithMetricsTags() {
        String pool = "smt-tags-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, SAMPLE_TAGS);

        tracker.recordConnectionAcquiredNanos(100_000_000L);
        tracker.recordConnectionUsageMillis(200L);
        tracker.recordConnectionCreatedMillis(30L);
        tracker.recordConnectionTimeout();

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

    // Close lifecycle

    @Test
    void testCloseUnregistersPoolMetrics() {
        String pool = "smt-close-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, pool, Map.of());
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(pool));

        SqlMetricsTracker tracker = new SqlMetricsTracker(pool, Map.of());
        tracker.close();
        assertFalse(ObservabilityUtils.hasRegisteredMetrics(pool),
                "close() must unregister pool metrics");
    }

    @Test
    void testCloseIsIdempotent() {
        String pool = "smt-close-idem-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, pool, Map.of());

        SqlMetricsTracker tracker = new SqlMetricsTracker(pool, Map.of());
        tracker.close();
        tracker.close();
        assertFalse(ObservabilityUtils.hasRegisteredMetrics(pool));
    }

    // Record after close (no-op path)

    @Test
    void testRecordAfterCloseDoesNotCreateCacheEntries() {
        String pool = "smt-closed-noop-pool";
        SqlMetricsTracker tracker = new SqlMetricsTracker(pool, Map.of());
        tracker.close();

        tracker.recordConnectionAcquiredNanos(100_000_000L);
        tracker.recordConnectionUsageMillis(50L);
        tracker.recordConnectionCreatedMillis(30L);
        tracker.recordConnectionTimeout();

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

    // Helpers

    private static PoolStats createMockPoolStats(int active, int idle,
                                                  int total, int pending,
                                                  int max, int min) {
        return new ObservabilityUtilsTest.MutableMockPoolStats(
                active, idle, total, pending, max, min);
    }
}
