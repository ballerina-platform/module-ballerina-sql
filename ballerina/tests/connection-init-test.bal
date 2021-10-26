// Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/test;

string connectDB = urlPrefix + "9001/connection";

@test:BeforeGroups {
    value: ["connection"]
}
function initConnectionContainer() returns error? {
    check initializeDockerContainer("sql-connection", "connection", "9001", "connection", "connector-init-test-data.sql");
    return;
}

@test:AfterGroups {
    value: ["connection"]
}
function cleanConnectionContainer() returns error? {
    check cleanDockerContainer("sql-connection");
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnection1() returns error? {
    MockClient testDB = check new (url = connectDB, user = user, password = password);
    test:assertExactEquals(testDB.close(), (), "Initialising connection failure.");
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnection2() returns error? {
    MockClient testDB = check new (connectDB, user, password);
    test:assertExactEquals(testDB.close(), (), "Initialising connection failure.");
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnectionAfterClose() returns error? {
    MockClient testDB = check new (connectDB, user, password);
    check testDB.close();
    stream<record{}, error?> streamData = testDB->query(`SELECT * FROM Customers`);
    record {|record {} value;|}?|error data = streamData.next();
    test:assertTrue(data is error);
    if data is ApplicationError {
        test:assertTrue(data.message().startsWith("SQL Client is already closed, hence further operations are not " + 
            "allowed"));
    } else {
        test:assertFail("ApplicationError Error expected.");
    }

    ExecutionResult|Error result = testDB->execute(`INSERT INTO Customers (firstName) VALUES ('Peter')`);
    if result is Error {
        test:assertTrue(result.message().startsWith("SQL Client is already closed, hence further operations are not " + 
                    "allowed"));
    }

    ExecutionResult[]|Error result2 = testDB->batchExecute([`INSERT INTO Customers (firstName) VALUES ('Peter')`]);
    if result2 is Error {
        test:assertTrue(result2.message().startsWith("SQL Client is already closed, hence further operations are not " + 
                    "allowed"));
    }

    ProcedureCallResult|Error result3 = testDB->call(`call MockProcedure()`);
    if result3 is Error {
        test:assertTrue(result3.message().startsWith("SQL Client is already closed, hence further operations are not " + 
                        "allowed"));
    }

    int|Error result4 = testDB->queryRow(`SELECT * FROM Customers`);
    if result is Error {
        test:assertTrue(result.message().startsWith("SQL Client is already closed, hence further operations are not " + 
                    "allowed"));
    }

    return;
}

@test:Config {
    groups: ["connection"]
}
function testStreamNextAfterClose() returns error? {
    MockClient testDB = check new (connectDB, user, password);
    stream<record{}, error?> streamData = testDB->query(`SELECT * FROM Customers`);
    var iterator = streamData.iterator();
    check streamData.close();
    record {|record {} value;|}?|error data = iterator.next();
    test:assertTrue(data is error);
    if data is ApplicationError {
        test:assertTrue(data.message().startsWith("Stream is closed. Therefore, no operations are allowed further " + 
        "on the stream."));
    } else {
        test:assertFail("ApplicationError Error expected.");
    }
    check testDB.close();
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnectionInvalidUrl() returns error? {
    string invalidUrl = urlPrefix;
    MockClient|Error dbClient = new (invalidUrl);
    if !(dbClient is Error) {
        check dbClient.close();
        test:assertFail("Invalid does not throw DatabaseError");
    }
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnectionNoUserPassword() returns error? {
    MockClient|Error dbClient = new (connectDB);
    if !(dbClient is Error) {
        check dbClient.close();
        test:assertFail("No username does not throw DatabaseError");
    }
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnectionWithValidDriver() returns error? {
    MockClient|Error dbClient = new (connectDB, user, password, "org.hsqldb.jdbc.JDBCDataSource");
    if dbClient is Error {
        test:assertFail("Valid driver throws DatabaseError");
    } else {
        check dbClient.close();
    }
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnectionWithInvalidDriver() returns error? {
    MockClient|Error dbClient = new (connectDB, user, password, 
        "org.hsqldb.jdbc.JDBCDataSourceInvalid");
    if !(dbClient is Error) {
        check dbClient.close();
        test:assertFail("Invalid driver does not throw DatabaseError");
    }
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnectionWithDatasourceOptions() returns error? {
    MockClient|Error dbClient = new (connectDB, user, password, "org.hsqldb.jdbc.JDBCDataSource", 
        {"loginTimeout": 5000});
    if dbClient is Error {
        test:assertFail("Datasource options throws DatabaseError");
    } else {
        check dbClient.close();
    }
    return;
}

@test:Config {
    groups: ["connection"]
}
function testConnectionWithDatasourceInvalidProperty() returns error? {
    MockClient|Error dbClient = new (connectDB, user, password, "org.hsqldb.jdbc.JDBCDataSource", 
        {"invalidProperty": 10});
    if dbClient is Error {
        test:assertEquals(dbClient.message(), 
        "Error in SQL connector configuration: Property invalidProperty does not exist on target class org.hsqldb.jdbc.JDBCDataSource");
    } else {
        check dbClient.close();
        test:assertFail("Invalid driver does not throw DatabaseError");
    }
    return;
}

@test:Config {
    groups: ["connection"]
}
function testWithConnectionPool() returns error? {
    ConnectionPool connectionPool = {
        maxOpenConnections: 25
    };
    MockClient dbClient = check new (url = connectDB, user = user, 
        password = password, connectionPool = connectionPool);
    error? err = dbClient.close();
    if err is error {
        test:assertFail("DB connection not created properly.");
    } else {
        test:assertEquals(connectionPool.maxConnectionLifeTime, <decimal>2000.5);
        test:assertEquals(connectionPool.minIdleConnections, 5);
    }
    return;
}

@test:Config {
    groups: ["connection"]
}
function testWithSharedConnPool() returns error? {
    ConnectionPool connectionPool = {
        maxOpenConnections: 25
    };
    MockClient dbClient1 = check new (url = connectDB, user = user, 
        password = password, connectionPool = connectionPool);
    MockClient dbClient2 = check new (url = connectDB, user = user, 
        password = password, connectionPool = connectionPool);
    MockClient dbClient3 = check new (url = connectDB, user = user, 
        password = password, connectionPool = connectionPool);

    test:assertEquals(dbClient1.close(), (), "HSQLDB connection failure.");
    test:assertEquals(dbClient2.close(), (), "HSQLDB connection failure.");
    test:assertEquals(dbClient3.close(), (), "HSQLDB connection failure.");

    return;
}

@test:Config {
    groups: ["connection"]
}
function testWithAllParams() returns error? {
    ConnectionPool connectionPool = {
        maxOpenConnections: 25
    };
    MockClient dbClient = check new (connectDB, user, password, "org.hsqldb.jdbc.JDBCDataSource", 
        {"loginTimeout": 5000}, connectionPool);
    test:assertEquals(dbClient.close(), (), "HSQLDB connection failure.");

    return;
}

@test:Config {
    groups: ["connection"]
}
isolated function testGenerateErrorStream() returns error? {
    stream<record {}, Error?> errorStream = generateApplicationErrorStream("Test generate Error Stream");
    record {}|Error? firstElement = errorStream.next();
    test:assertTrue(firstElement is Error);
    return;
}
