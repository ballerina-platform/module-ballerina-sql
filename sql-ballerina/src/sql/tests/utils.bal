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
import ballerina/filepath;
import ballerina/runtime;
import ballerina/system;
import ballerina/test;

string scriptPath = checkpanic filepath:absolute("src/sql/tests/resources/sql");

string user = "test";
string password = "";
string urlPrefix = "jdbc:hsqldb:hsql://localhost:";

function initializeDockerContainer(string containerName, string dbAlias, string port, string resFolder, string scriptName) {
    system:Process process;
    int exitCode = 1;
    process = checkpanic system:exec(
        "docker", {}, scriptPath, "run", "--rm", "-d", "--name", containerName,
        "-e", "HSQLDB_DATABASE_ALIAS=" + dbAlias,
        "-e", "HSQLDB_USER=test",
        "-v", scriptPath + "/" + resFolder + ":/scripts",
        "-p", port + ":9001", "blacklabelops/hsqldb"
    );
    exitCode = checkpanic process.waitForExit();
    test:assertEquals(exitCode, 0, "Docker container '" + containerName + "' start failed");

    runtime:sleep(20000);

    int counter = 0;
    exitCode = 1;
    while (exitCode > 0 && counter < 12) {
        runtime:sleep(5000);
        process = checkpanic system:exec(
            "docker", {}, scriptPath, "exec", containerName,
            "java", "-jar", "/opt/hsqldb/sqltool.jar", 
            "--autoCommit",
            "--inlineRc", "url=" + urlPrefix + "9001/" +  dbAlias + ",user=test,password=", 
            "/scripts/" + scriptName
        );
        exitCode = checkpanic process.waitForExit();
        counter = counter + 1;
    }
    test:assertExactEquals(exitCode, 0, "Docker container '" + containerName + "' health test exceeded timeout!");
}

function cleanDockerContainer(string containerName) {
    system:Process process = checkpanic system:exec("docker", {}, scriptPath, "stop", containerName);
    int exitCode = checkpanic process.waitForExit();
    test:assertExactEquals(exitCode, 0, "Docker container '" + containerName + "' stop failed!");
}

function getByteColumnChannel() returns @untainted io:ReadableByteChannel {
    io:ReadableByteChannel byteChannel = checkpanic io:openReadableFile("./src/sql/tests/resources/files/byteValue.txt");
    return byteChannel;
}

function getBlobColumnChannel() returns @untainted io:ReadableByteChannel {
    io:ReadableByteChannel byteChannel = checkpanic io:openReadableFile("./src/sql/tests/resources/files/blobValue.txt");
    return byteChannel;
}

function getClobColumnChannel() returns @untainted io:ReadableCharacterChannel {
    io:ReadableByteChannel byteChannel = checkpanic io:openReadableFile("./src/sql/tests/resources/files/clobValue.txt");
    io:ReadableCharacterChannel sourceChannel = new (byteChannel, "UTF-8");
    return sourceChannel;
}

function getUntaintedData(record {}|error? value, string fieldName) returns @untainted anydata {
    if (value is record {}) {
        return value[fieldName];
    }
    return {};
}

function queryMockClient(string url, @untainted string|ParameterizedQuery sqlQuery)
returns @tainted record {}? {
    MockClient dbClient = checkpanic new (url = url, user = user, password = password);
    stream<record{}, error> streamData = dbClient->query(sqlQuery);
    record {|record {} value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();
    record {}? value = data?.value;
    checkpanic dbClient.close();
    return value;
}
