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

/**
 * Constants for SQL connection pool and request observability metrics.
 *
 * @since 1.18.0
 */
public final class ObservabilityConstants {

    private ObservabilityConstants() {
    }

    // Tag keys
    public static final String TAG_POOL_NAME = "pool_name";

    // Pool health metric names (Gauges)
    public static final String METRIC_POOL_ACTIVE_CONNECTIONS =
            "sql_pool_active_connections";
    public static final String METRIC_POOL_IDLE_CONNECTIONS =
            "sql_pool_idle_connections";
    public static final String METRIC_POOL_TOTAL_CONNECTIONS =
            "sql_pool_total_connections";
    public static final String METRIC_POOL_PENDING_REQUESTS =
            "sql_pool_pending_requests";
    public static final String METRIC_POOL_MAX_CONNECTIONS =
            "sql_pool_max_connections";
    public static final String METRIC_POOL_MIN_CONNECTIONS =
            "sql_pool_min_connections";
    public static final String METRIC_POOL_UTILIZATION_RATIO =
            "sql_pool_utilization_ratio";
    public static final String METRIC_POOL_INIT_TIME =
            "sql_pool_initialization_time_seconds";

    // Connection event metric names (3 Summarized Gauges + 1 Counter)
    public static final String METRIC_CONNECTION_ACQUISITION_TIME =
            "sql_connection_acquisition_time_seconds";
    public static final String METRIC_CONNECTION_USAGE_TIME =
            "sql_connection_usage_time_seconds";
    public static final String METRIC_CONNECTION_CREATION_TIME =
            "sql_connection_creation_time_seconds";
    public static final String METRIC_CONNECTION_TIMEOUT_TOTAL =
            "sql_connection_timeout_total";

    // Pool health metric descriptions
    static final String DESC_POOL_ACTIVE =
            "Connections currently borrowed from pool";
    static final String DESC_POOL_IDLE =
            "Connections available in pool";
    static final String DESC_POOL_TOTAL =
            "Active + idle (total pool size)";
    static final String DESC_POOL_PENDING =
            "Application threads waiting for a connection";
    static final String DESC_POOL_MAX =
            "Configured maximum pool size";
    static final String DESC_POOL_MIN =
            "Configured minimum idle connections";
    static final String DESC_POOL_UTILIZATION =
            "Active / max connection utilization ratio";
    static final String DESC_POOL_INIT_TIME =
            "Time taken to initialize the connection pool in seconds";

    // Connection event metric descriptions
    static final String DESC_CONN_ACQUISITION =
            "Time a thread waited to get a connection from the pool";
    static final String DESC_CONN_USAGE =
            "Time a connection was held before being returned";
    static final String DESC_CONN_CREATION =
            "Time to create a new physical connection";
    static final String DESC_CONN_TIMEOUT =
            "Connection acquisitions that timed out";

    // Unit conversion constants
    static final double NANOS_TO_SECONDS = 1_000_000_000.0;
    static final double MILLIS_TO_SECONDS = 1_000.0;
}
