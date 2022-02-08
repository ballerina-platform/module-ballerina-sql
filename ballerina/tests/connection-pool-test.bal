// Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/lang.runtime as runtime;
import ballerina/lang.'string as strings;
import ballerina/test;

string poolDB_1 = urlPrefix + "9002/Pool1";
string poolDB_2 = urlPrefix + "9003/Pool2";

public type Result record {
    int val;
};

string datasourceName = "org.hsqldb.jdbc.JDBCDataSource";
map<anydata> connectionPoolOptions = {
    "connectionTimeout": "1000"
};

@test:BeforeGroups {
    value: ["pool"]
}
function initPoolContainer() returns error? {
    check initializeDockerContainer("sql-pool1", "Pool1", "9002", "pool", "connection-pool-test-data.sql");
    check initializeDockerContainer("sql-pool2", "Pool2", "9003", "pool", "connection-pool-test-data.sql");
}

@test:AfterGroups {
    value: ["pool"]
}
function cleanPoolContainer() returns error? {
    check cleanDockerContainer("sql-pool1");
    check cleanDockerContainer("sql-pool2");
}

@test:Config {
    groups: ["pool"]
}
function testGlobalConnectionPoolSingleDestination() returns error? {
    check drainGlobalPool(poolDB_1);
}

@test:Config {
    groups: ["pool"]
}
function testGlobalConnectionPoolsMultipleDestinations() returns error? {
    check drainGlobalPool(poolDB_1);
    check drainGlobalPool(poolDB_2);
}

@test:Config {
    groups: ["pool"]
}
function testGlobalConnectionPoolSingleDestinationConcurrent() returns error? {
    worker w1 returns [stream<Result, error?>, stream<Result, error?>]|error {
        return testGlobalConnectionPoolConcurrentHelper1(poolDB_1);
    }

    worker w2 returns [stream<Result, error?>, stream<Result, error?>]|error {
        return testGlobalConnectionPoolConcurrentHelper1(poolDB_1);
    }

    worker w3 returns [stream<Result, error?>, stream<Result, error?>]|error {
        return testGlobalConnectionPoolConcurrentHelper1(poolDB_1);
    }

    worker w4 returns [stream<Result, error?>, stream<Result, error?>]|error {
        return testGlobalConnectionPoolConcurrentHelper1(poolDB_1);
    }

    record {
        [stream<Result, error?>, stream<Result, error?>]|error w1;
        [stream<Result, error?>, stream<Result, error?>]|error w2;
        [stream<Result, error?>, stream<Result, error?>]|error w3;
        [stream<Result, error?>, stream<Result, error?>]|error w4;
    } results = wait {w1, w2, w3, w4};

    (int|error)[] result2 = check testGlobalConnectionPoolConcurrentHelper2(poolDB_1);

    (int|error)[][] returnArray = [];
    // Connections will be released here as we fully consume the data in the following conversion function calls
    returnArray[0] = check getCombinedReturnValue(results.w1);
    returnArray[1] = check getCombinedReturnValue(results.w2);
    returnArray[2] = check getCombinedReturnValue(results.w3);
    returnArray[3] = check getCombinedReturnValue(results.w4);
    returnArray[4] = result2;

    // All 5 clients are supposed to use the same pool. Default maximum no of connections is 10.
    // Since each select operation hold up one connection each, the last select operation should
    // return an error
    int i = 0;
    while (i < 4) {
        if returnArray[i][0] is anydata {
            test:assertEquals(returnArray[i][0], 1);
            if returnArray[i][1] is anydata {
                test:assertEquals(returnArray[i][1], 1);
            } else {
                test:assertFail("Expected second element of array an integer" + (<error>returnArray[i][1]).message());
            }
        } else {
            test:assertFail("Expected first element of array an integer" + (<error>returnArray[i][0]).message());
        }
        i = i + 1;
    }
    validateConnectionTimeoutError(result2[2]);
}

@test:Config {
    groups: ["pool"]
}
function testLocalSharedConnectionPoolConfigSingleDestination() returns error? {
    ConnectionPool pool = {maxOpenConnections: 5};
    MockClient dbClient1 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient2 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient3 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient4 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient5 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);

    (stream<Result, error?>)[] resultArray = [];
    resultArray[0] = dbClient1->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[1] = dbClient2->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[2] = dbClient3->query(`select count(*) as val from Customers where registrationID = 2`);
    resultArray[3] = dbClient4->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[4] = dbClient5->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[5] = dbClient5->query(`select count(*) as val from Customers where registrationID = 2`);

    (int|error)[] returnArray = [];
    int i = 0;
    // Connections will be released here as we fully consume the data in the following conversion function calls
    foreach stream<Result, error?> x in resultArray {
        returnArray[i] = getReturnValue(x);
        i += 1;
    }

    check dbClient1.close();
    check dbClient2.close();
    check dbClient3.close();
    check dbClient4.close();
    check dbClient5.close();

    // All 5 clients are supposed to use the same pool created with the configurations given by the
    // custom pool options. Since each select operation holds up one connection each, the last select
    // operation should return an error
    i = 0;
    while (i < 5) {
        test:assertEquals(returnArray[i], 1);
        i = i + 1;
    }
    validateConnectionTimeoutError(returnArray[5]);
}

@test:Config {
    groups: ["pool"]
}
function testLocalSharedConnectionPoolConfigDifferentDbOptions() returns error? {
    ConnectionPool pool = {maxOpenConnections: 3};
    MockClient dbClient1 = check new (url = poolDB_1, user = user, password = password, 
        datasourceName = datasourceName, options = {"loginTimeout": "2000"}, connectionPool = pool, 
        connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient2 = check new (url = poolDB_1, user = user, password = password, 
        datasourceName = datasourceName, options = {"loginTimeout": "2000"}, connectionPool = pool, 
        connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient3 = check new (url = poolDB_1, user = user, password = password, 
        datasourceName = datasourceName, options = {"loginTimeout": "2000"}, connectionPool = pool, 
        connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient4 = check new (url = poolDB_1, user = user, password = password, 
        datasourceName = datasourceName, options = {"loginTimeout": "1000"}, connectionPool = pool, 
        connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient5 = check new (url = poolDB_1, user = user, password = password, 
        datasourceName = datasourceName, options = {"loginTimeout": "1000"}, connectionPool = pool, 
        connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient6 = check new (url = poolDB_1, user = user, password = password, 
        datasourceName = datasourceName, options = {"loginTimeout": "1000"}, connectionPool = pool, 
        connectionPoolOptions = connectionPoolOptions);

    stream<Result, error?>[] resultArray = [];
    resultArray[0] = dbClient1->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[1] = dbClient2->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[2] = dbClient3->query(`select count(*) as val from Customers where registrationID = 2`);
    resultArray[3] = dbClient3->query(`select count(*) as val from Customers where registrationID = 1`);

    resultArray[4] = dbClient4->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[5] = dbClient5->query(`select count(*) as val from Customers where registrationID = 2`);
    resultArray[6] = dbClient6->query(`select count(*) as val from Customers where registrationID = 2`);
    resultArray[7] = dbClient6->query(`select count(*) as val from Customers where registrationID = 1`);

    (int|error)[] returnArray = [];
    int i = 0;
    // Connections will be released here as we fully consume the data in the following conversion function calls
    foreach stream<Result, error?> x in resultArray {
        returnArray[i] = getReturnValue(x);
        i += 1;
    }

    check dbClient1.close();
    check dbClient2.close();
    check dbClient3.close();
    check dbClient4.close();
    check dbClient5.close();
    check dbClient6.close();

    // Since max pool size is 3, the last select function call going through each pool should fail.
    i = 0;
    while (i < 3) {
        test:assertEquals(returnArray[i], 1);
        test:assertEquals(returnArray[i + 4], 1);
        i = i + 1;
    }
    validateConnectionTimeoutError(returnArray[3]);
    validateConnectionTimeoutError(returnArray[7]);

}

@test:Config {
    groups: ["pool"]
}
function testLocalSharedConnectionPoolConfigMultipleDestinations() returns error? {
    ConnectionPool pool = {maxOpenConnections: 3};
    MockClient dbClient1 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient2 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient3 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient4 = check new (url = poolDB_2, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient5 = check new (url = poolDB_2, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient6 = check new (url = poolDB_2, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);

    stream<Result, error?>[] resultArray = [];
    resultArray[0] = dbClient1->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[1] = dbClient2->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[2] = dbClient3->query(`select count(*) as val from Customers where registrationID = 2`);
    resultArray[3] = dbClient3->query(`select count(*) as val from Customers where registrationID = 1`);

    resultArray[4] = dbClient4->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[5] = dbClient5->query(`select count(*) as val from Customers where registrationID = 2`);
    resultArray[6] = dbClient6->query(`select count(*) as val from Customers where registrationID = 2`);
    resultArray[7] = dbClient6->query(`select count(*) as val from Customers where registrationID = 1`);

    (int|error)[] returnArray = [];
    int i = 0;
    // Connections will be released here as we fully consume the data in the following conversion function calls
    foreach stream<Result, error?> x in resultArray {
        returnArray[i] = getReturnValue(x);
        i += 1;
    }

    check dbClient1.close();
    check dbClient2.close();
    check dbClient3.close();
    check dbClient4.close();
    check dbClient5.close();
    check dbClient6.close();

    // Since max pool size is 3, the last select function call going through each pool should fail.
    i = 0;
    while (i < 3) {
        test:assertEquals(returnArray[i], 1);
        test:assertEquals(returnArray[i + 4], 1);
        i = i + 1;
    }
    validateConnectionTimeoutError(returnArray[3]);
    validateConnectionTimeoutError(returnArray[7]);
}

@test:Config {
    groups: ["pool"]
}
function testLocalSharedConnectionPoolCreateClientAfterShutdown() returns error? {
    ConnectionPool pool = {maxOpenConnections: 2};
    MockClient dbClient1 = check new (url = poolDB_1, user = user, password = password, connectionPoolOptions = connectionPoolOptions, connectionPool = pool);
    MockClient dbClient2 = check new (url = poolDB_1, user = user, password = password, connectionPoolOptions = connectionPoolOptions, connectionPool = pool);

    stream<Result, error?> dt1 = dbClient1->query(`SELECT count(*) as val from Customers where registrationID = 1`);
    stream<Result, error?> dt2 = dbClient2->query(`SELECT count(*) as val from Customers where registrationID = 1`);
    int|error result1 = getReturnValue(dt1);
    int|error result2 = getReturnValue(dt2);

    // Since both clients are stopped the pool is supposed to shutdown.
    check dbClient1.close();
    check dbClient2.close();

    // This call should return an error as pool is shutdown
    stream<Result, error?> dt3 = dbClient1->query(`SELECT count(*) as val from Customers where registrationID = 1`);
    int|error result3 = getReturnValue(dt3);

    // Now a new pool should be created
    MockClient dbClient3 = check new (url = poolDB_1, user = user, password = password, connectionPoolOptions = connectionPoolOptions, connectionPool = pool);

    // This call should be successful
    stream<Result, error?> dt4 = dbClient3->query(`SELECT count(*) as val from Customers where registrationID = 1`);
    int|error result4 = getReturnValue(dt4);

    check dbClient3.close();

    test:assertEquals(result1, 1);
    test:assertEquals(result2, 1);
    validateApplicationError(result3);
    test:assertEquals(result4, 1);
}

ConnectionPool pool1 = {maxOpenConnections: 2};

@test:Config {
    groups: ["pool"]
}
function testLocalSharedConnectionPoolStopInitInterleave() returns error? {
    worker w1 returns error? {
        check testLocalSharedConnectionPoolStopInitInterleaveHelper1(poolDB_1);
    }
    worker w2 returns int|error {
        return testLocalSharedConnectionPoolStopInitInterleaveHelper2(poolDB_1);
    }

    check wait w1;
    int|error result = wait w2;
    test:assertEquals(result, 1);
}

function testLocalSharedConnectionPoolStopInitInterleaveHelper1(string url) 
returns error? {
    MockClient dbClient = check new (url = url, user = user, password = password, connectionPool = pool1, 
        connectionPoolOptions = connectionPoolOptions);
    runtime:sleep(1);
    check dbClient.close();
}

function testLocalSharedConnectionPoolStopInitInterleaveHelper2(string url) 
returns int|error {
    runtime:sleep(1);
    MockClient dbClient = check new (url = url, user = user, password = password, connectionPool = pool1, 
        connectionPoolOptions = connectionPoolOptions);
    stream<Result, error?> dt = dbClient->query(`SELECT COUNT(*) as val from Customers where registrationID = 1`);
    int|error count = getReturnValue(dt);
    check dbClient.close();
    return count;
}

@test:Config {
    groups: ["pool"]
}
function testShutDownUnsharedLocalConnectionPool() returns error? {
    ConnectionPool pool = {maxOpenConnections: 2};
    MockClient dbClient = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);

    stream<Result, error?> result = dbClient->query(`select count(*) as val from Customers where registrationID = 1`);
    int|error retVal1 = getReturnValue(result);
    // Pool should be shutdown as the only client using it is stopped.
    check dbClient.close();
    // This should result in an error return.
    stream<Result, error?> resultAfterPoolShutDown = dbClient->query(`select count(*) as val from Customers where registrationID = 1`);
    int|error retVal2 = getReturnValue(resultAfterPoolShutDown);

    test:assertEquals(retVal1, 1);
    validateApplicationError(retVal2);
}

@test:Config {
    groups: ["pool"]
}
function testShutDownSharedConnectionPool() returns error? {
    ConnectionPool pool = {maxOpenConnections: 1};
    MockClient dbClient1 = check new (url = poolDB_1, user = user, password = password, connectionPoolOptions = connectionPoolOptions, connectionPool = pool);
    MockClient dbClient2 = check new (url = poolDB_1, user = user, password = password, connectionPoolOptions = connectionPoolOptions, connectionPool = pool);

    stream<Result, error?> result1 = dbClient1->query(`select count(*) as val from Customers where registrationID = 1`);
    int|error retVal1 = getReturnValue(result1);

    stream<Result, error?> result2 = dbClient2->query(`select count(*) as val from Customers where registrationID = 2`);
    int|error retVal2 = getReturnValue(result2);

    // Only one client is closed so pool should not shutdown.
    check dbClient1.close();

    // This should be successful as pool is still up.
    stream<Result, error?> result3 = dbClient2->query(`select count(*) as val from Customers where registrationID = 2`);
    int|error retVal3 = getReturnValue(result3);

    // This should fail because, even though the pool is up, this client was stopped
    stream<Result, error?> result4 = dbClient1->query(`select count(*) as val from Customers where registrationID = 2`);
    int|error retVal4 = getReturnValue(result4);

    // Now pool should be shutdown as the only remaining client is stopped.
    check dbClient2.close();

    // This should fail because this client is stopped.
    stream<Result, error?> result5 = dbClient2->query(`select count(*) as val from Customers where registrationID = 2`);
    int|error retVal5 = getReturnValue(result5);

    test:assertEquals(retVal1, 1);
    test:assertEquals(retVal2, 1);
    test:assertEquals(retVal3, 1);
    validateApplicationError(retVal4);
    validateApplicationError(retVal5);
}

@test:Config {
    groups: ["pool"]
}
function testShutDownPoolCorrespondingToASharedPoolConfig() returns error? {
    ConnectionPool pool = {maxOpenConnections: 1};
    MockClient dbClient1 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient2 = check new (url = poolDB_1, user = user, password = password, connectionPool = pool, connectionPoolOptions = connectionPoolOptions);

    stream<Result, error?> result1 = dbClient1->query(`select count(*) as val from Customers where registrationID = 1`);
    int|error retVal1 = getReturnValue(result1);

    stream<Result, error?> result2 = dbClient2->query(`select count(*) as val from Customers where registrationID = 2`);
    int|error retVal2 = getReturnValue(result2);

    // This should result in stopping the pool used by this client as it was the only client using that pool.
    check dbClient1.close();

    // This should be successful as the pool belonging to this client is up.
    stream<Result, error?> result3 = dbClient2->query(`select count(*) as val from Customers where registrationID = 2`);
    int|error retVal3 = getReturnValue(result3);

    // This should fail because this client was stopped.
    stream<Result, error?> result4 = dbClient1->query(`select count(*) as val from Customers where registrationID = 2`);
    int|error retVal4 = getReturnValue(result4);

    check dbClient2.close();

    test:assertEquals(retVal1, 1);
    test:assertEquals(retVal2, 1);
    test:assertEquals(retVal3, 1);
    validateApplicationError(retVal4);
}

@test:Config {
    groups: ["pool"]
}
function testStopClientUsingGlobalPool() returns error? {
    // This client doesn't have pool config specified therefore, global pool will be used.
    MockClient dbClient = check new (url = poolDB_1, user = user, password = password, connectionPoolOptions = connectionPoolOptions);

    stream<Result, error?> result1 = dbClient->query(`select count(*) as val from Customers where registrationID = 1`);
    int|error retVal1 = getReturnValue(result1);

    // This will merely stop this client and will not have any effect on the pool because it is the global pool.
    check dbClient.close();

    // This should fail because this client was stopped, even though the pool is up.
    stream<Result, error?> result2 = dbClient->query(`select count(*) as val from Customers where registrationID = 1`);
    int|error retVal2 = getReturnValue(result2);

    test:assertEquals(retVal1, 1);
    validateApplicationError(retVal2);
}

@test:Config {
    groups: ["pool"]
}
function testLocalConnectionPoolShutDown() {
    int|error count1 = getOpenConnectionCount(poolDB_1);
    int|error count2 = getOpenConnectionCount(poolDB_2);
    if count1 is error {
        if count2 is error {
            test:assertEquals(count1.message(), count2.message());
        } else {
            test:assertFail("Expected invalid count of connection pool" + count2.toString());
        }
    } else {
        test:assertFail("Expected invalid count of connection pool" + count1.toString());
    }
}

public type Variable record {
    string value;
    string variable_name;
};

function getOpenConnectionCount(string url) returns (int|error) {
    MockClient dbClient = check new (url = url, user = user, password = password, connectionPool = {maxOpenConnections: 1}, connectionPoolOptions = connectionPoolOptions);
    stream<Variable, error?> dt = dbClient->query(`show status where variable_name = 'Threads_connected'`);
    int|error count = getIntVariableValue(dt);
    check dbClient.close();
    return count;
}

function testGlobalConnectionPoolConcurrentHelper1(string url) returns [stream<Result, error?>, stream<Result, error?>]|error {
    MockClient dbClient = check new (url = url, user = user, password = password, connectionPoolOptions = connectionPoolOptions);
    stream<Result, error?> dt1 = dbClient->query(`select count(*) as val from Customers where registrationID = 1`);
    stream<Result, error?> dt2 = dbClient->query(`select count(*) as val from Customers where registrationID = 2`);
    return [dt1, dt2];
}

function testGlobalConnectionPoolConcurrentHelper2(string url) returns (int|error)[]|error {
    MockClient dbClient = check new (url = url, user = user, password = password, connectionPoolOptions = connectionPoolOptions);
    (int|error)[] returnArray = [];
    stream<Result, error?> dt1 = dbClient->query(`select count(*) as val from Customers where registrationID = 1`);
    stream<Result, error?> dt2 = dbClient->query(`select count(*) as val from Customers where registrationID = 2`);
    stream<Result, error?> dt3 = dbClient->query(`select count(*) as val from Customers where registrationID = 1`);
    // Connections will be released here as we fully consume the data in the following conversion function calls
    returnArray[0] = getReturnValue(dt1);
    returnArray[1] = getReturnValue(dt2);
    returnArray[2] = getReturnValue(dt3);

    return returnArray;
}

isolated function getCombinedReturnValue([stream<Result, error?>, stream<Result, error?>]|error queryResult)
    returns (int|error)[]|error {
    if queryResult is error {
        return queryResult;
    } else {
        stream<Result, error?> x;
        stream<Result, error?> y;
        [x, y] = queryResult;
        (int|error)[] returnArray = [];
        returnArray[0] = getReturnValue(x);
        returnArray[1] = getReturnValue(y);
        return returnArray;
    }
}

isolated function getIntVariableValue(stream<record {}, error?> queryResult) returns int|error {
    int count = -1;
    record {|record {} value;|}? data = check queryResult.next();
    if data is record {|record {} value;|} {
        record {} variable = data.value;
        if variable is Variable {
            return 'int:fromString(variable.value);
        }
    }
    check queryResult.close();
    return count;
}

function drainGlobalPool(string url) returns error? {
    MockClient dbClient1 = check new (url = url, user = user, password = password, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient2 = check new (url = url, user = user, password = password, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient3 = check new (url = url, user = user, password = password, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient4 = check new (url = url, user = user, password = password, connectionPoolOptions = connectionPoolOptions);
    MockClient dbClient5 = check new (url = url, user = user, password = password, connectionPoolOptions = connectionPoolOptions);

    stream<Result, error?>[] resultArray = [];

    resultArray[0] = dbClient1->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[1] = dbClient1->query(`select count(*) as val from Customers where registrationID = 2`);

    resultArray[2] = dbClient2->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[3] = dbClient2->query(`select count(*) as val from Customers where registrationID = 1`);

    resultArray[4] = dbClient3->query(`select count(*) as val from Customers where registrationID = 2`);
    resultArray[5] = dbClient3->query(`select count(*) as val from Customers where registrationID = 2`);

    resultArray[6] = dbClient4->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[7] = dbClient4->query(`select count(*) as val from Customers where registrationID = 1`);

    resultArray[8] = dbClient5->query(`select count(*) as val from Customers where registrationID = 1`);
    resultArray[9] = dbClient5->query(`select count(*) as val from Customers where registrationID = 1`);

    resultArray[10] = dbClient5->query(`select count(*) as val from Customers where registrationID = 1`);

    (int|error)[] returnArray = [];
    int i = 0;
    // Connections will be released here as we fully consume the data in the following conversion function calls
    foreach stream<Result, error?> x in resultArray {
        returnArray[i] = getReturnValue(x);

        i += 1;
    }
    // All 5 clients are supposed to use the same pool. Default maximum no of connections is 10.
    // Since each select operation hold up one connection each, the last select operation should
    // return an error
    i = 0;
    while (i < 10) {
        test:assertEquals(returnArray[i], 1);
        i = i + 1;
    }
    validateConnectionTimeoutError(returnArray[10]);
}

isolated function getReturnValue(stream<Result, error?> queryResult) returns int|error {
    record {|Result value;|}? data = check queryResult.next();
    check queryResult.close();
    if data is () {
        return -1;
    } else {
        Result value = data.value;
        return value.val;
    }
}

isolated function validateApplicationError(int|error dbError) {
    test:assertTrue(dbError is error);
    ApplicationError sqlError = <ApplicationError>dbError;
    test:assertTrue(strings:includes(sqlError.message(), "Client is already closed"), sqlError.message());
}

isolated function validateConnectionTimeoutError(int|error dbError) {
    test:assertTrue(dbError is error);
    DatabaseError sqlError = <DatabaseError>dbError;
    test:assertTrue(strings:includes(sqlError.message(), "request timed out after"), sqlError.message());
}
