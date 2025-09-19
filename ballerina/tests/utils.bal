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

import ballerina/file;
import ballerina/io;
import ballerina/lang.runtime as runtime;
import ballerina/os;
import ballerina/test;

const decimal DOCKER_START_TIMEOUT = 20;
const int DOCKER_RETRY_COUNT = 15;
const decimal DOCKER_RETRY_DELAY = 5;

string scriptPath = check file:getAbsolutePath("tests/resources/sql");

string user = "test";
string password = "";
string urlPrefix = "jdbc:hsqldb:hsql://localhost:";

function initializeDockerContainer(string containerName, string dbAlias, string port, string resFolder,
        string scriptName) returns error? {
    os:Process result = check os:exec({
                                          value: "docker",
                                          arguments: [
                                              "run",
                                              "--rm",
                                              "-d",
                                              "--name",
                                              containerName,
                                              "-e",
                                              "HSQLDB_DATABASE_ALIAS=" + dbAlias,
                                              "-e",
                                              "HSQLDB_USER=test",
                                              "-v",
                                              check file:joinPath(scriptPath, resFolder) + ":/scripts",
                                              "-p",
                                              port + ":9001",
                                              "kaneeldias/hsqldb"
                                          ]
                                      });
    int exitCode = check result.waitForExit();
    if exitCode > 0 {
        return error(string `Docker container '${containerName}' failed to start. Exit code: ${exitCode}`);
    }
    io:println(string `Docker container for Database '${dbAlias}' created.`);
    runtime:sleep(DOCKER_START_TIMEOUT);

    int counter = 0;
    while counter < DOCKER_RETRY_COUNT {
        result = check os:exec({
                                   value: "docker",
                                   arguments: [
                                       "exec",
                                       containerName,
                                       "java",
                                       "-jar",
                                       "/opt/hsqldb/sqltool.jar",
                                       "--autoCommit",
                                       "--inlineRc",
                                       "url=" + urlPrefix + "9001/" + dbAlias + ",user=test,password=",
                                       "/scripts/" + scriptName
                                   ]
                               });
        exitCode = check result.waitForExit();
        if exitCode == 0 {
            break;
        }
        counter = counter + 1;
        runtime:sleep(DOCKER_RETRY_DELAY);
    }
    test:assertExactEquals(exitCode, 0, string `Docker container '${containerName}' health test exceeded timeout!`);
    io:println(string `Docker container for Database '${dbAlias}' initialized with the script.`);
}

function cleanDockerContainer(string containerName) returns error? {
    os:Process result = check os:exec({
                                          value: "docker",
                                          arguments: [
                                              "stop",
                                              containerName
                                          ]
                                      });
    int exitCode = check result.waitForExit();
    test:assertExactEquals(exitCode, 0, string `Docker container '${containerName}' stop failed!`);
    io:println(string `Cleaned docker container '${containerName}'.`);
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

isolated function getUntaintedData(record {}? value, string fieldName) returns anydata {
    if value is record {} {
        return value[fieldName];
    }
    return {};
}

function getMockClient(string url) returns MockClient|error {
    MockClient dbClient = check new (url = url, user = user, password = password);
    return dbClient;
}

function queryMockClient(string url, ParameterizedQuery sqlQuery)
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
