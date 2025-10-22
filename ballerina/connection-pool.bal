// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/jballerina.java;

# The maximum number of open connections that the pool is allowed to have.
public configurable int maxOpenConnections = 15;

# The maximum lifetime (in seconds) of a connection in the pool.
public configurable decimal maxConnectionLifeTime = 1800.0;

# The minimum number of idle connections that the pool tries to maintain.
public configurable int minIdleConnections = 15;

# Maximum time (in seconds) to wait for a connection from the pool before failing.
public configurable decimal connectionTimeout = 30.0;

# Maximum time (in seconds) an idle connection is kept before retirement.
public configurable decimal idleTimeout = 600.0;

# Maximum time (in seconds) allowed for a connection validation.
public configurable decimal validationTimeout = 5.0;

# Threshold (in seconds) to flag a potential connection leak (use `0` to disable leak detection).
public configurable decimal leakDetectionThreshold = 0.0;

# Interval (in seconds) to periodically keep idle connections alive (use `0` to disable keep alive).
public configurable decimal keepAliveTime = 0.0;

# Pool name for logs/metrics. If unset, an internal name is used.
public configurable string? poolName = ();

# Controls pool boot behavior: fail fast vs. lazy init. Use negative value to disable failure timeout.
public configurable decimal initializationFailTimeout = 1.0;

# Default transaction isolation for connections. If unset, driver default is used.
# Allowed values typically: `TRANSACTION_READ_UNCOMMITTED`, `TRANSACTION_READ_COMMITTED`,
# `TRANSACTION_REPEATABLE_READ`, `TRANSACTION_SERIALIZABLE`.
public configurable TransactionIsolation? transactionIsolation = ();

# SQL query to validate a connection. Leave unset to use driver-native validation.
public configurable string? connectionTestQuery = ();

# SQL executed when a new connection is created. Useful for session-level settings.
public configurable readonly & (string|string[]?) connectionInitSql = ();

# Marks connections as read-only by default. Use with care when apps issue writes.
public configurable boolean readOnly = false;

# Allows suspending the pool for maintenance. Rarely needed; keep disabled.
public configurable boolean allowPoolSuspension = false;

# Isolates pool's internal queries from application transactions.
public configurable boolean isolateInternalQueries = false;

public type ConnectionPool record {|
    # The maximum number of open connections that the pool is allowed to have. Includes both idle and in-use
    # connections. The default value is 15. This can be changed through the configuration API with the
    # `ballerina.sql.maxOpenConnections` key
    int maxOpenConnections = maxOpenConnections;
    # The maximum lifetime (in seconds) of a connection in the pool. The default value is 1800 seconds
    # (30 minutes). A value of 0 indicates an unlimited maximum lifetime (infinite lifetime). The minimum
    # allowed value is 30 seconds. This can be changed through the configuration API with the
    # `ballerina.sql.maxConnectionLifeTime` key.
    decimal maxConnectionLifeTime = maxConnectionLifeTime;
    # The minimum number of idle connections that the pool tries to maintain. The default value is the same as
    # `maxOpenConnections` and it can be changed through the configuration API with the
    # `ballerina.sql.minIdleConnections` key
    int minIdleConnections = minIdleConnections;
    # Maximum time (in seconds) to wait for a connection from the pool before failing. The default value is
    # 30 seconds. This can be changed through the configuration API with the `ballerina.sql.connectionTimeout` key
    decimal connectionTimeout = connectionTimeout;
    # Maximum time (in seconds) an idle connection is kept before retirement. The default value is 600 seconds
    # (10 minutes). This can be changed through the configuration API with the `ballerina.sql.idleTimeout` key
    decimal idleTimeout = idleTimeout;
    # Maximum time (in seconds) allowed for a connection validation. The default value is 5 seconds. This
    # can be changed through the configuration API with the `ballerina.sql.validationTimeout` key
    decimal validationTimeout = validationTimeout;
    # Threshold (in seconds) to flag a potential connection leak (use `0` to disable leak detection). The default
    # value is 0 seconds. This can be changed through the configuration API with the
    # `ballerina.sql.leakDetectionThreshold` key
    decimal leakDetectionThreshold = leakDetectionThreshold;
    # Interval (in seconds) to periodically keep idle connections alive (use `0` to disable keep alive). The
    # default value is 0 seconds. This can be changed through the configuration API with the
    # `ballerina.sql.keepAliveTime` key
    decimal keepAliveTime = keepAliveTime;
    # Pool name for logs/metrics. If unset, an internal name is used. The default value is nil. This can be
    # changed through the configuration API with the `ballerina.sql.poolName` key
    string? poolName = poolName;
    # Controls pool boot behavior: fail fast vs. lazy init. Use negative value to disable failure timeout. The
    # default value is 1 second. This can be changed through the configuration API with the
    # `ballerina.sql.initializationFailTimeout` key
    decimal initializationFailTimeout = initializationFailTimeout;
    # Default transaction isolation for connections. If unset, driver default is used. Allowed values typically:
    # `TRANSACTION_READ_UNCOMMITTED`, `TRANSACTION_READ_COMMITTED`, `TRANSACTION_REPEATABLE_READ`,
    # `TRANSACTION_SERIALIZABLE`.
    # This can be changed through the configuration API with the `ballerina.sql.transactionIsolation` key
    TransactionIsolation? transactionIsolation = transactionIsolation;
    # SQL query to validate a connection. Leave unset to use driver-native validation. The default value is
    # nil. This can be changed through the configuration API with the `ballerina.sql.connectionTestQuery` key
    string? connectionTestQuery = connectionTestQuery;
    # SQL executed when a new connection is created. Useful for session-level settings. The default value is
    # nil. This can be changed through the configuration API with the `ballerina.sql.connectionInitSql` key
    string|string[]? connectionInitSql = connectionInitSql;
    # Marks connections as read-only by default. Use with care when apps issue writes. The default value is
    # false. This can be changed through the configuration API with the `ballerina.sql.readOnly` key
    boolean readOnly = readOnly;
    # Allows suspending the pool for maintenance. Rarely needed; keep disabled. The default value is false.
    # This can be changed through the configuration API with the `ballerina.sql.allowPoolSuspension` key
    boolean allowPoolSuspension = allowPoolSuspension;
    # Isolates pool's internal queries from application transactions. The default value is false. This can
    # be changed through the configuration API with the `ballerina.sql.isolateInternalQueries` key
    boolean isolateInternalQueries = isolateInternalQueries;
|};

// A container object that holds the global pool config and initializes the internal map of connection pools
readonly class GlobalConnectionPoolContainer {
    private ConnectionPool connectionPool = {};

    isolated function init() {
        // poolConfig record is frozen so that it cannot be modified during runtime
        ConnectionPool frozenConfig = self.connectionPool.cloneReadOnly();
        initGlobalPoolContainer(frozenConfig);
    }

    public isolated function getGlobalConnectionPool() returns ConnectionPool {
        return self.connectionPool;
    }
}

isolated function initGlobalPoolContainer(ConnectionPool poolConfig) = @java:Method {
    'class: "io.ballerina.stdlib.sql.utils.ConnectionPoolUtils"
} external;

// This is an instance of GlobalPoolConfigContainer object type. The init functions of database clients pass
// poolConfig member of this instance to the external client creation logic in order to access the internal map
// of connection pools.
final GlobalConnectionPoolContainer globalPoolContainer = new;

# Returns the global connection pool.
#
# + return - The connection pool
public isolated function getGlobalConnectionPool() returns ConnectionPool {
    return globalPoolContainer.getGlobalConnectionPool();
}
