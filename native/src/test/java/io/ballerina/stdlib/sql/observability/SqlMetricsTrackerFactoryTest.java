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

import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;
import org.testng.annotations.Test;

import java.util.Map;

import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_ACQUISITION_TIME;
import static io.ballerina.stdlib.sql.observability.ObservabilityUtils.METRIC_CONNECTION_TIMEOUT_TOTAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link SqlMetricsTrackerFactory}.
 * Verifies name resolution, pool metric registration, and
 * tracker creation.
 *
 * @since 1.19.0
 */
public class SqlMetricsTrackerFactoryTest {

    private static final Map<String, String> SAMPLE_TAGS = Map.of(
            ObservabilityUtils.TAG_DB_HOST, "localhost",
            ObservabilityUtils.TAG_DB_PORT, "5432",
            ObservabilityUtils.TAG_DB_NAME, "testdb");

    // Name resolution

    @Test
    void testGetRegisteredPoolNameBeforeCreate() {
        SqlMetricsTrackerFactory factory =
                new SqlMetricsTrackerFactory("my-pool", Map.of());
        assertNull(factory.getRegisteredPoolName(),
                "Must return null before create() is called");
    }

    @Test
    void testCreateWithNullMetricPoolNameUsesHikariName() {
        SqlMetricsTrackerFactory factory =
                new SqlMetricsTrackerFactory(null, Map.of());
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);

        factory.create("HikariPool-1", stats);

        assertEquals(factory.getRegisteredPoolName(), "HikariPool-1",
                "Must use HikariCP name when metricPoolName is null");
        ObservabilityUtils.unregisterPoolMetrics("HikariPool-1");
    }

    @Test
    void testCreateWithUserMetricPoolName() {
        SqlMetricsTrackerFactory factory =
                new SqlMetricsTrackerFactory("my-pool", Map.of());
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);

        factory.create("HikariPool-2", stats);

        assertEquals(factory.getRegisteredPoolName(), "my-pool",
                "Must use user-configured metricPoolName");
        ObservabilityUtils.unregisterPoolMetrics("my-pool");
    }

    // Pool metric registration

    @Test
    void testCreateRegistersPoolMetrics() {
        String pool = "smtf-register-pool";
        SqlMetricsTrackerFactory factory =
                new SqlMetricsTrackerFactory(pool, SAMPLE_TAGS);
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);

        factory.create(pool, stats);

        assertTrue(ObservabilityUtils.hasRegisteredMetrics(pool),
                "create() must register pool health metrics");
        assertNotNull(ObservabilityUtils.getPoolGauges(pool));
        assertEquals(ObservabilityUtils.getPoolGauges(pool).size(), 7);

        ObservabilityUtils.unregisterPoolMetrics(pool);
    }

    // Tracker functionality

    @Test
    void testCreateReturnsWorkingTracker() {
        String pool = "smtf-tracker-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        SqlMetricsTrackerFactory factory =
                new SqlMetricsTrackerFactory(pool, SAMPLE_TAGS);

        IMetricsTracker tracker = factory.create(pool, stats);

        tracker.recordConnectionAcquiredNanos(100_000_000L);
        assertNotNull(ObservabilityUtils.getCachedGauge(
                METRIC_CONNECTION_ACQUISITION_TIME, pool));

        tracker.recordConnectionTimeout();
        assertNotNull(ObservabilityUtils.getCachedCounter(
                METRIC_CONNECTION_TIMEOUT_TOTAL, pool));

        tracker.close();
    }

    // Helpers

    private static PoolStats createMockPoolStats(int active, int idle,
                                                  int total, int pending,
                                                  int max, int min) {
        return new ObservabilityUtilsTest.MutableMockPoolStats(
                active, idle, total, pending, max, min);
    }
}
