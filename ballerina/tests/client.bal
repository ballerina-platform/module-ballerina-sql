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

# Represents a Mock database client.
isolated client class MockClient {
    *Client;

    public function init(string url, string? user = (), string? password = (), string? datasourceName = (),
        map<anydata>? options = (), ConnectionPool? connectionPool = (),
        map<anydata>? connectionPoolOptions = ()) returns Error? {
        SQLParams sqlParams = {
            url,
            user,
            password,
            datasourceName,
            options,
            connectionPool,
            connectionPoolOptions
        };
        return createSqlClient(self, sqlParams, getGlobalConnectionPool());
    }

    remote isolated function query(ParameterizedQuery sqlQuery, typedesc<record {}> rowType = <>)
    returns stream<rowType, Error?> = @java:Method {
        'class: "io.ballerina.stdlib.sql.testutils.QueryTestUtils",
        name: "nativeQuery"
    } external;

    remote isolated function queryRow(ParameterizedQuery sqlQuery, typedesc<anydata> returnType = <>)
    returns returnType|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.testutils.QueryTestUtils",
        name: "nativeQueryRow"
    } external;

    remote isolated function execute(ParameterizedQuery sqlQuery) returns ExecutionResult|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.testutils.ExecuteTestUtils",
        name: "nativeExecute"
    } external;

    remote isolated function batchExecute(ParameterizedQuery[] sqlQueries) returns ExecutionResult[]|Error {
        if sqlQueries.length() == 0 {
            return error ApplicationError(" Parameter 'sqlQueries' cannot be empty array");
        }
        return nativeBatchExecute(self, sqlQueries);
    }

    remote isolated function call(ParameterizedCallQuery sqlQuery, typedesc<record {}>[] rowTypes = [])
    returns ProcedureCallResult|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.testutils.CallTestUtils",
        name: "nativeCall"
    } external;

    public isolated function close() returns Error? = @java:Method {
        'class: "io.ballerina.stdlib.sql.testutils.ClientTestUtils",
        name: "close"
    } external;
}

type SQLParams record {|
    string? url;
    string? user;
    string? password;
    string? datasourceName;
    map<anydata>? options;
    ConnectionPool? connectionPool;
    map<anydata>? connectionPoolOptions;
|};

function createSqlClient(Client sqlClient, SQLParams sqlParams, ConnectionPool globalConnPool)
returns Error? = @java:Method {
    'class: "io.ballerina.stdlib.sql.testutils.ClientTestUtils"
} external;

isolated function nativeBatchExecute(Client sqlClient, ParameterizedQuery[] sqlQueries)
returns ExecutionResult[]|Error = @java:Method {
    'class: "io.ballerina.stdlib.sql.testutils.ExecuteTestUtils"
} external;
