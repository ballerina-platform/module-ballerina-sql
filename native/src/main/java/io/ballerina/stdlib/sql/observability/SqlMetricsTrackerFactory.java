/*
 * Copyright (c) 2025, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
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
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;

/**
 * HikariCP MetricsTrackerFactory that bridges pool metrics to Ballerina's observe module.
 * <p>
 * Set on HikariConfig before pool creation when metrics are enabled. HikariCP calls
 * {@link #create(String, PoolStats)} once when the pool starts, providing the pool name
 * and a PoolStats object for reading pool state on demand.
 *
 * @since 1.18.0
 */
public class SqlMetricsTrackerFactory implements MetricsTrackerFactory {

    private final String metricPoolName;

    public SqlMetricsTrackerFactory(String metricPoolName) {
        this.metricPoolName = metricPoolName;
    }

    @Override
    public IMetricsTracker create(String poolName, PoolStats poolStats) {
        try {
            ObservabilityUtils.registerPoolMetrics(poolStats, metricPoolName);
        } catch (Exception e) {
            // Silently swallow — pool must start even if metrics registration fails
        }
        return new SqlMetricsTracker(metricPoolName);
    }
}
