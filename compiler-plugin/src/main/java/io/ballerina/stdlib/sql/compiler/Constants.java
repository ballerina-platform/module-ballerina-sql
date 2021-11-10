/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.ballerina.stdlib.sql.compiler;

/**
 * Constants used in compiler plugin.
 */
public class Constants {
    public static final String BALLERINA = "ballerina";
    public static final String SQL = "sql";
    public static final String CONNECTION_POOL = "ConnectionPool";

    /**
     * Constants for fields in sql:ConnectionPool.
     */
    public static class ConnectionPool {
        public static final String MAX_OPEN_CONNECTIONS = "maxOpenConnections";
        public static final String MAX_CONNECTION_LIFE_TIME = "maxConnectionLifeTime";
        public static final String MIN_IDLE_CONNECTIONS = "minIdleConnections";
    }

    public static final String UNNECESSARY_CHARS_REGEX = "\"|\\n";

}
