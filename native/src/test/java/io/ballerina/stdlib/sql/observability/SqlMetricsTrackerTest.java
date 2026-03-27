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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link SqlMetricsTracker}.
 *
 * @since 1.18.0
 */
public class SqlMetricsTrackerTest {

    // ---- Record methods (open tracker) ----

    @Test
    void testRecordConnectionAcquiredNanos() {
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                "smt-acq-pool", JdbcUrlInfo.EMPTY);
        tracker.recordConnectionAcquiredNanos(500_000_000L);
        ObservabilityUtils.unregisterPoolMetrics("smt-acq-pool");
    }

    @Test
    void testRecordConnectionUsageMillis() {
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                "smt-usage-pool", JdbcUrlInfo.EMPTY);
        tracker.recordConnectionUsageMillis(150L);
        ObservabilityUtils.unregisterPoolMetrics("smt-usage-pool");
    }

    @Test
    void testRecordConnectionCreatedMillis() {
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                "smt-create-pool", JdbcUrlInfo.EMPTY);
        tracker.recordConnectionCreatedMillis(50L);
        ObservabilityUtils.unregisterPoolMetrics("smt-create-pool");
    }

    @Test
    void testRecordConnectionTimeout() {
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                "smt-timeout-pool", JdbcUrlInfo.EMPTY);
        tracker.recordConnectionTimeout();
        ObservabilityUtils.unregisterPoolMetrics("smt-timeout-pool");
    }

    // ---- Record methods with URL info tags ----

    @Test
    void testRecordWithUrlInfoTags() {
        JdbcUrlInfo urlInfo = new JdbcUrlInfo(
                "dbhost", "5432", "mydb",
                "jdbc:postgresql://dbhost:5432/mydb");
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                "smt-url-pool", urlInfo);
        tracker.recordConnectionAcquiredNanos(100_000_000L);
        tracker.recordConnectionUsageMillis(200L);
        tracker.recordConnectionCreatedMillis(30L);
        tracker.recordConnectionTimeout();
        ObservabilityUtils.unregisterPoolMetrics("smt-url-pool");
    }

    // ---- Close lifecycle ----

    @Test
    void testCloseUnregistersPoolMetrics() {
        String pool = "smt-close-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(
                stats, pool, JdbcUrlInfo.EMPTY);
        assertTrue(ObservabilityUtils.hasRegisteredMetrics(pool));

        SqlMetricsTracker tracker = new SqlMetricsTracker(
                pool, JdbcUrlInfo.EMPTY);
        tracker.close();
        assertFalse(ObservabilityUtils.hasRegisteredMetrics(pool));
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
        tracker.close();
    }

    // ---- Record after close (no-op path) ----

    @Test
    void testRecordAfterCloseIsNoOp() {
        SqlMetricsTracker tracker = new SqlMetricsTracker(
                "smt-closed-noop-pool", JdbcUrlInfo.EMPTY);
        tracker.close();

        tracker.recordConnectionAcquiredNanos(100_000_000L);
        tracker.recordConnectionUsageMillis(50L);
        tracker.recordConnectionCreatedMillis(30L);
        tracker.recordConnectionTimeout();
    }

    // ---- Factory integration ----

    @Test
    void testFactoryCreatesWorkingTracker() {
        String pool = "smt-factory-pool";
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        SqlMetricsTrackerFactory factory =
                new SqlMetricsTrackerFactory(pool,
                        "jdbc:postgresql://localhost:5432/mydb");
        IMetricsTracker tracker = factory.create(pool, stats);

        tracker.recordConnectionAcquiredNanos(100_000_000L);
        tracker.recordConnectionUsageMillis(50L);
        tracker.recordConnectionCreatedMillis(30L);
        tracker.recordConnectionTimeout();
        tracker.close();
    }

    // ---- Helpers ----

    private static PoolStats createMockPoolStats(int active, int idle,
                                                  int total, int pending,
                                                  int max, int min) {
        return new PoolStats(1000L) {
            {
                this.activeConnections = active;
                this.idleConnections = idle;
                this.totalConnections = total;
                this.pendingThreads = pending;
                this.maxConnections = max;
                this.minConnections = min;
            }

            @Override
            protected void update() {
            }
        };
    }
}
