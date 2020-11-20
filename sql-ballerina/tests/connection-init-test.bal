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
function initConnectionContainer() {
	initializeDockerContainer("sql-connection", "connection", "9001", "connection", "connector-init-test-data.sql");
}

@test:AfterGroups {
	value: ["connection"]	
} 
function cleanConnectionContainer() {
	cleanDockerContainer("sql-connection");
}

@test:Config {
    groups: ["connection"]
}
function testConnection1() {
    MockClient testDB = checkpanic new (url = connectDB, user = user, password = password);
    test:assertExactEquals(testDB.close(), (), "Initialising connection failure.");
}

@test:Config {
    groups: ["connection"]
}
function testConnection2() {
    MockClient testDB = checkpanic new (connectDB, user, password);
    test:assertExactEquals(testDB.close(), (), "Initialising connection failure.");
}

@test:Config {
    groups: ["connection"]
}
function testConnectionInvalidUrl() {
    string invalidUrl = urlPrefix;
    MockClient|Error dbClient = new (invalidUrl);
    if (!(dbClient is Error)) {
        checkpanic dbClient.close();
        test:assertFail("Invalid does not throw DatabaseError");
    } 
}

@test:Config {
    groups: ["connection"]
}
function testConnectionNoUserPassword() {
    MockClient|Error dbClient = new (connectDB);
    if (!(dbClient is Error)) {
        checkpanic dbClient.close();
        test:assertFail("No username does not throw DatabaseError");
    } 
}

@test:Config {
    groups: ["connection"]
}
function testConnectionWithValidDriver() {
    MockClient|Error dbClient = new (connectDB, user, password, "org.hsqldb.jdbc.JDBCDataSource");
    if (dbClient is Error) {
        test:assertFail("Valid driver throws DatabaseError");
    } else {
        checkpanic dbClient.close();
    }
}

@test:Config {
    groups: ["connection"]
}
function testConnectionWithInvalidDriver() {
    MockClient|Error dbClient = new (connectDB, user, password,
        "org.hsqldb.jdbc.JDBCDataSourceInvalid");
    if (!(dbClient is Error)) {
        checkpanic dbClient.close();
        test:assertFail("Invalid driver does not throw DatabaseError");
    }
}

@test:Config {
    groups: ["connection"]
}
function testConnectionWithDatasourceOptions() {
    MockClient|Error dbClient = new (connectDB, user, password, "org.hsqldb.jdbc.JDBCDataSource",
        {"loginTimeout": 5000});
    if (dbClient is Error) {
        test:assertFail("Datasource options throws DatabaseError");
    } else {
        checkpanic dbClient.close();
    }
}

@test:Config {
    groups: ["connection"]
}
function testConnectionWithDatasourceInvalidProperty() {
    MockClient|Error dbClient = new (connectDB, user, password, "org.hsqldb.jdbc.JDBCDataSource",
        {"invalidProperty": 10});
    if (dbClient is Error) {
        test:assertEquals(dbClient.message(), 
        "Error in SQL connector configuration: Property invalidProperty does not exist on target class org.hsqldb.jdbc.JDBCDataSource");
    } else {
        checkpanic dbClient.close();
        test:assertFail("Invalid driver does not throw DatabaseError");
    }
}

@test:Config {
    groups: ["connection"]
}
function testWithConnectionPool() {
    ConnectionPool connectionPool = {
        maxOpenConnections: 25
    };
    MockClient dbClient = checkpanic new (url = connectDB, user = user,
        password = password, connectionPool = connectionPool);
    error? err = dbClient.close();
    if (err is error) {
        test:assertFail("DB connection not created properly.");
    } else {
        test:assertEquals(connectionPool.maxConnectionLifeTimeInSeconds, <decimal> 2000.5);
        test:assertEquals(connectionPool.minIdleConnections, 5);
    }
}

@test:Config {
    groups: ["connection"]
}
function testWithSharedConnPool() {
    ConnectionPool connectionPool = {
        maxOpenConnections: 25
    };
    MockClient dbClient1 = checkpanic new (url = connectDB, user = user,
        password = password, connectionPool = connectionPool);
    MockClient dbClient2 = checkpanic new (url = connectDB, user = user,
        password = password, connectionPool = connectionPool);
    MockClient dbClient3 = checkpanic new (url = connectDB, user = user,
        password = password, connectionPool = connectionPool);

    test:assertEquals(dbClient1.close(), (), "HSQLDB connection failure.");
    test:assertEquals(dbClient2.close(), (), "HSQLDB connection failure.");
    test:assertEquals(dbClient3.close(), (), "HSQLDB connection failure.");
}

@test:Config {
    groups: ["connection"]
}
function testWithAllParams() {
    ConnectionPool connectionPool = {
        maxOpenConnections: 25
    };
    MockClient dbClient = checkpanic new (connectDB, user, password, "org.hsqldb.jdbc.JDBCDataSource",
        {"loginTimeout": 5000}, connectionPool);
    test:assertEquals(dbClient.close(), (), "HSQLDB connection failure.");
}
