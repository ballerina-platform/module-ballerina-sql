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

import ballerina/io;
import ballerina/file;
import ballerina/lang.runtime as runtime;
import ballerina/test;
import ballerina/jballerina.java;

string scriptPath = check file:getAbsolutePath("tests/resources/sql");

string user = "test";
string password = "";
string urlPrefix = "jdbc:hsqldb:hsql://localhost:";

@test:BeforeSuite
isolated function beforeSuite() {
    setModuleForTest();
    io:println("Test suite initiated");
}

@test:AfterSuite {}
isolated function afterSuite() {
    io:println("Test suite finished");
}

function initializeDockerContainer(string containerName, string dbAlias, string port, string resFolder, 
        string scriptName) returns error? {
    int exitCode = 1;
    Process|error execResult = exec("docker", {}, scriptPath, "run", "--rm", 
        "-d", "--name", containerName, 
        "-e", "HSQLDB_DATABASE_ALIAS=" + dbAlias, 
        "-e", "HSQLDB_USER=test", 
        "-v", check file:joinPath(scriptPath, resFolder) + ":/scripts", 
        "-p", port + ":9001", "kaneeldias/hsqldb");
    Process result = check execResult;
    int waitForExit = check result.waitForExit();
    exitCode = check result.exitCode();
    test:assertEquals(exitCode, 0, "Docker container '" + containerName + "' failed to start");
    io:println("Docker container for Database '" + dbAlias + "' created.");
    runtime:sleep(20);

    int counter = 0;
    exitCode = 1;
    while (exitCode > 0 && counter < 12) {
        runtime:sleep(5);
        execResult = exec(
            "docker", {}, scriptPath, "exec", containerName, 
            "java", "-jar", "/opt/hsqldb/sqltool.jar", 
            "--autoCommit", 
            "--inlineRc", "url=" + urlPrefix + "9001/" + dbAlias + ",user=test,password=", 
            "/scripts/" + scriptName
        );
        result = check execResult;
        waitForExit = check result.waitForExit();
        exitCode = check result.exitCode();
        counter = counter + 1;
    }
    test:assertExactEquals(exitCode, 0, "Docker container '" + containerName + "' health test exceeded timeout!");
    io:println("Docker container for Database '" + dbAlias + "' initialised with the script.");
}

function cleanDockerContainer(string containerName) returns error? {
    Process|error execResult = exec("docker", {}, scriptPath, "stop", containerName);
    Process result = check execResult;
    int waitForExit = check result.waitForExit();

    int exitCode = check result.exitCode();
    test:assertExactEquals(exitCode, 0, "Docker container '" + containerName + "' stop failed!");
    io:println("Cleaned docker container '" + containerName + "'.");
}

isolated function getByteColumnChannel() returns io:ReadableByteChannel|error {
    io:ReadableByteChannel byteChannel = check io:openReadableFile("./tests/resources/files/byteValue.txt");
    return byteChannel;
}

isolated function getBlobColumnChannel() returns io:ReadableByteChannel|error {
    io:ReadableByteChannel byteChannel = check io:openReadableFile("./tests/resources/files/blobValue.txt");
    return byteChannel;
}

isolated function getClobColumnChannel() returns io:ReadableCharacterChannel|error {
    io:ReadableByteChannel byteChannel = check io:openReadableFile("./tests/resources/files/clobValue.txt");
    io:ReadableCharacterChannel sourceChannel = new (byteChannel, "UTF-8");
    return sourceChannel;
}

isolated function getUntaintedData(record {}|error? value, string fieldName) returns anydata {
    if (value is record {}) {
        return value[fieldName];
    }
    return {};
}

function getMockClient(string url) returns MockClient|error {
    MockClient dbClient = check new (url = url, user = user, password = password);
    return dbClient;
}

function queryMockClient(string url, string|ParameterizedQuery sqlQuery) 
returns record {}|error? {
    MockClient dbClient = check getMockClient(url);
    stream<record {}, error?> streamData = dbClient->query(sqlQuery);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();
    return value;
}

function queryRecordMockClient(string url, ParameterizedQuery sqlQuery)
returns record {}|error {
    MockClient dbClient = check getMockClient(url);
    record {} resultRecord = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    return resultRecord;
}

function exec(string command, map<string> env = {}, 
                    string? dir = (), string... args) returns Process|error = @java:Method {
    name: "exec",
    'class: "io.ballerina.stdlib.sql.testutils.nativeimpl.Exec"
} external;

isolated function setModuleForTest() = @java:Method {
    name: "setModule",
    'class: "io.ballerina.stdlib.sql.testutils.nativeimpl.ModuleUtils"
} external;
