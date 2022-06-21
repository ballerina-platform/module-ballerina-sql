// Copyright (c) 2021 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/test;
//import ballerina/io;

string metadataDb = urlPrefix + "9017/metadata";

@test:BeforeGroups {
    value: ["metadata"]
}
function initMetadataContainer() returns error? {
    //check initializeDockerContainer("sql-metadata", "metadata", "9017", "metadata", "metadata-test-data.sql");
}

@test:AfterGroups {
    value: ["metadata"]
}
function cleanMetadataContainer() returns error? {
    //check cleanDockerContainer("sql-metadata");
}

@test:Config {
    groups: ["metadata"]
}
function listTablesTest() returns error? {
    MockSchemaClient schemaClient = check new(url = metadataDb, user = user, password = password, database = "PUBLIC");
    string[] tables = check schemaClient->listTables();
    test:assertEquals(tables, ["CUSTOMERS","DATATABLE","NUMERICTYPES","CUSTOMERNAMES"]);
}

@test:Config {
    groups: ["metadata"]
}
function getTableInfoTest() returns error? {
    MockSchemaClient schemaClient = check new(url = metadataDb, user = user, password = password, database = "PUBLIC");
    TableDefinition 'table = check schemaClient->getTableInfo("CUSTOMERS");
    test:assertEquals('table, {
        name: "CUSTOMERS",
        "type":"BASE_TABLE",
        "columns":[
            {
                "name":"CUSTOMERID",
                "type":"INTEGER",
                "defaultValue":null,
                "nullable":false
            },
            {
                "name":"FIRSTNAME",
                "type":"CHARACTER VARYING",
                "defaultValue":null,
                "nullable":false
            },
            {
                "name":"LASTNAME",
                "type":"CHARACTER VARYING",
                "defaultValue":null,
                "nullable":false
            },
            {
                "name":"REGISTRATIONID",
                "type":"INTEGER",
                "defaultValue":null,
                "nullable":false
            },
            {   
                "name":"CREDITLIMIT",
                "type":"DOUBLE PRECISION",
                "defaultValue":"100.00",
                "nullable":false
            },
            {
                "name":"COUNTRY",
                "type":"CHARACTER VARYING",
                "defaultValue":null,
                "nullable":false
            }
        ]
    });
}