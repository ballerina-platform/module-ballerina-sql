// Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

string errorDB = urlPrefix + "9013/error";

@test:BeforeGroups {
	value: ["error"]
}
function initErrorContainer() returns error? {
	check initializeDockerContainer("sql-error", "error", "9013", "error", "error-database-init.sql");
}

@test:AfterGroups {
	value: ["error"]
}
function cleanErrorContainer() returns error? {
	check cleanDockerContainer("sql-error");
}

@test:Config {
    groups: ["error"]
}
function queryCorruptedJson() returns error? {
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = 1`;
    MockClient mockClient = check getMockClient(errorDB);
    json|Error jsonVal = mockClient->queryRow(sqlQuery);
    test:assertTrue(jsonVal is ConversionError);
}
