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

/**
 * HikariCP IMetricsTracker that records connection event metrics via ObservabilityUtils.
 * <p>
 * HikariCP calls the record methods on every connection event (acquisition, usage, creation,
 * timeout). The {@link #close()} method is called on pool shutdown and triggers metric
 * unregistration.
 *
 * @since 1.18.0
 */
public class SqlMetricsTracker implements IMetricsTracker {

    private final String poolName;
    private final JdbcUrlInfo urlInfo;

    SqlMetricsTracker(String poolName, JdbcUrlInfo urlInfo) {
        this.poolName = poolName;
        this.urlInfo = urlInfo;
    }

    @Override
    public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
        ObservabilityUtils.recordConnectionAcquisitionTime(
                poolName, elapsedAcquiredNanos, urlInfo);
    }

    @Override
    public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
        ObservabilityUtils.recordConnectionUsageTime(
                poolName, elapsedBorrowedMillis, urlInfo);
    }

    @Override
    public void recordConnectionCreatedMillis(long connectionCreatedMillis) {
        ObservabilityUtils.recordConnectionCreationTime(
                poolName, connectionCreatedMillis, urlInfo);
    }

    @Override
    public void recordConnectionTimeout() {
        ObservabilityUtils.recordConnectionTimeout(poolName, urlInfo);
    }

    @Override
    public void close() {
        try {
            ObservabilityUtils.unregisterPoolMetrics(poolName);
        } catch (Exception e) {
            // Silently swallow — shutdown must complete even if unregister fails
        }
    }
}
