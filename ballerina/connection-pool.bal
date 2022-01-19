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

configurable int maxOpenConnections = 15;
configurable decimal maxConnectionLifeTime = 1800.0;
configurable int minIdleConnections = 15;

# Represents the properties, which are used to configure a DB connection pool.
# Default values of the fields can be set through the configuration API.
#
# + maxOpenConnections - The maximum number of open connections that the pool is allowed to have.
#                        Includes both idle and in-use connections. The default value is 15. This can be changed through
#                        the configuration API with the `ballerina.sql.maxOpenConnections` key
# + maxConnectionLifeTime - The maximum lifetime (in seconds) of a connection in the pool. The default value is 1800
#                           seconds (30 minutes). This can be changed through the configuration API with the
#                           `ballerina.sql.maxConnectionLifeTime` key. A value of 0 indicates an unlimited maximum
#                           lifetime (infinite lifetime)
# + minIdleConnections - The minimum number of idle connections that the pool tries to maintain. The default value
#                        is the same as `maxOpenConnections` and it can be changed through the configuration
#                        API with the `ballerina.sql.minIdleConnections` key
public type ConnectionPool record {|
    int maxOpenConnections = maxOpenConnections;
    decimal maxConnectionLifeTime = maxConnectionLifeTime;
    int minIdleConnections = minIdleConnections;
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
