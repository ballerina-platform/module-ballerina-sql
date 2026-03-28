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
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;

import java.util.Map;

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
    private final Map<String, String> metricsTags;
    private String registeredPoolName;

    public SqlMetricsTrackerFactory(String metricPoolName,
                                    Map<String, String> metricsTags) {
        this.metricPoolName = metricPoolName;
        this.metricsTags = Map.copyOf(metricsTags);
    }

    @Override
    public IMetricsTracker create(String poolName, PoolStats poolStats) {
        String effectiveName = (metricPoolName != null)
                ? metricPoolName : poolName;
        this.registeredPoolName = effectiveName;
        try {
            ObservabilityUtils.registerPoolMetrics(poolStats, effectiveName,
                    metricsTags);
        } catch (Exception e) {
            // Silently swallow — pool must start even if metrics registration fails
        }
        return new SqlMetricsTracker(effectiveName, metricsTags);
    }

    /**
     * Return the pool name resolved during {@link #create}. When
     * {@code metricPoolName} was null, this is HikariCP's auto-generated name.
     *
     * @return the resolved pool name, or null if {@code create()} has not been called
     */
    public String getRegisteredPoolName() {
        return registeredPoolName;
    }
}
