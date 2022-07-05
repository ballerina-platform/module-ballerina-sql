// Copyright (c) 2022 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

string metadataDb = urlPrefix + "9017/metadata";

@test:BeforeGroups {
    value: ["metadata"]
}
function initMetadataContainer() returns error? {
    check initializeDockerContainer("sql-metadata", "metadata", "9017", "metadata", "metadata-test-data.sql");
    check createProcedures();
}

@test:AfterGroups {
    value: ["metadata"]
}
function cleanMetadataContainer() returns error? {
    check cleanDockerContainer("sql-metadata");
}

@test:Config {
    groups: ["metadata"]
}
function listTablesTest() returns error? {
    MockSchemaClient schemaClient = check new(url = metadataDb, user = user, password = password, database = "PUBLIC");
    string[] tables = check schemaClient->listTables();
    check schemaClient.close();
    test:assertEquals(tables, ["CUSTOMERS", "DATATABLE", "NUMERICTYPES", "CUSTOMERNAMES", "COMPANY", "PERSON", "STRINGTYPES"]);
}

@test:Config {
    groups: ["metadata"]
}
function getTableInfoTest() returns error? {
    MockSchemaClient schemaClient = check new(url = metadataDb, user = user, password = password, database = "PUBLIC");
    TableDefinition 'table = check schemaClient->getTableInfo("CUSTOMERS");
    check schemaClient.close();
    test:assertEquals('table, {
        name: "CUSTOMERS",
        "type":BASE_TABLE,
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
                "nullable":true
            },
            {
                "name":"COUNTRY",
                "type":"CHARACTER VARYING",
                "defaultValue":null,
                "nullable":true
            }
        ]
    });
}

@test:Config {
    groups: ["metadata"]
}
function getTableInfoWithConstraintsTest() returns error? {
    MockSchemaClient schemaClient = check new(url = metadataDb, user = user, password = password, database = "PUBLIC");
    TableDefinition 'table = check schemaClient->getTableInfo("PERSON", COLUMNS_WITH_CONSTRAINTS);
    check schemaClient.close();
    test:assertEquals('table, {
        name: "PERSON",
        "type":BASE_TABLE,
        "columns":[
            {
                "name":"PERSONID",
                "type":"INTEGER",
                "defaultValue":null,
                "nullable":false,
                "checkConstraints": [
                    {
                        "name":"SYS_CT_10125",
                        "clause":"PUBLIC.PERSON.PERSONID>100"
                    }
                ]
            },
            {
                "name":"NAME",
                "type":"CHARACTER VARYING",
                "defaultValue":null,
                "nullable":false
            },
            {
                "name":"COMPANYID",
                "type":"INTEGER",
                "defaultValue":null,
                "nullable":false,
                "referentialConstraints":[
                    {
                        "name":"SYS_FK_10124",
                        "tableName":"COMPANY",
                        "columnName":"COMPANYID",
                        "updateRule":NO_ACTION,
                        "deleteRule":NO_ACTION
                    }
                ],
                "checkConstraints": [
                    {
                        "name":"SYS_CT_10126",
                        "clause":"PUBLIC.PERSON.COMPANYID>20"
                    }
                ]
            }
        ]
    });
}

@test:Config {
    groups: ["metadata"]
}
function listRoutinesTest() returns error? {
    MockSchemaClient schemaClient = check new(url = metadataDb, user = user, password = password, database = "PUBLIC");
    string[] tables = check schemaClient->listRoutines();
    check schemaClient.close();
    test:assertEquals(tables, ["INSERTSTRINGDATA", "SELECTSTRINGDATAWITHOUTPARAMS"]);
}

@test:Config {
    groups: ["metadata"]
}
function getRoutineInfoTest() returns error? {
    MockSchemaClient schemaClient = check new(url = metadataDb, user = user, password = password, database = "PUBLIC");
    RoutineDefinition routine = check schemaClient->getRoutineInfo("INSERTSTRINGDATA");
    check schemaClient.close();
    test:assertEquals(routine, {
        "name":"INSERTSTRINGDATA",
        "type":"PROCEDURE",
        "returnType":null,
        "parameters":[
            {
                "mode":"IN",
                "name":"P_ID",
                "type":"INTEGER"
            },
            {
                "mode":"IN",
                "name":"P_VARCHAR_TYPE",
                "type":"CHARACTER VARYING"
            },
            {
                "mode":"IN",
                "name":"P_CHARMAX_TYPE",
                "type":"CHARACTER"
            },
            {
                "mode":"IN",
                "name":"P_CHAR_TYPE",
                "type":"CHARACTER"
            },
            {
                "mode":"IN",
                "name":"P_CHARACTERMAX_TYPE",
                "type":"CHARACTER"
            },
            {
                "mode":"IN",
                "name":"P_CHARACTER_TYPE",
                "type":"CHARACTER"
            },
            {
                "mode":"IN",
                "name":"P_NVARCHARMAX_TYPE",
                "type":"CHARACTER VARYING"
            }
        ]
    });
}

function createProcedures() returns error? {
    MockClient dbClient = check new (url = metadataDb, user = user, password = password);
    _ = check dbClient->execute(`
        CREATE PROCEDURE InsertStringData(
            IN p_id INTEGER,
            IN p_varchar_type VARCHAR(255),
            IN p_charmax_type CHAR(10),
            IN p_char_type CHAR,
            IN p_charactermax_type CHARACTER(10),
            IN p_character_type CHARACTER,
            IN p_nvarcharmax_type NVARCHAR(255))
        MODIFIES SQL DATA
            INSERT INTO StringTypes(id, varchar_type, charmax_type, char_type, charactermax_type, character_type, nvarcharmax_type)
            VALUES (p_id, p_varchar_type, p_charmax_type, p_char_type, p_charactermax_type, p_character_type, p_nvarcharmax_type);
    `);

    _ = check dbClient->execute(`
        CREATE PROCEDURE SelectStringDataWithOutParams (
            IN p_id INT, 
            OUT p_varchar_type VARCHAR(255),
            OUT p_charmax_type CHAR(10), 
            OUT p_char_type CHAR, 
            OUT p_charactermax_type CHARACTER(10),
            OUT p_character_type CHARACTER, 
            OUT p_nvarcharmax_type NVARCHAR(255)
        )
        READS SQL DATA DYNAMIC RESULT SETS 2
        BEGIN ATOMIC
            SELECT varchar_type INTO p_varchar_type FROM StringTypes where id = p_id;
            SELECT charmax_type INTO p_charmax_type FROM StringTypes where id = p_id;
            SELECT char_type INTO p_char_type FROM StringTypes where id = p_id;
            SELECT charactermax_type INTO p_charactermax_type FROM StringTypes where id = p_id;
            SELECT character_type INTO p_character_type FROM StringTypes where id = p_id;
            SELECT nvarcharmax_type INTO p_nvarcharmax_type FROM StringTypes where id = p_id;
        END
    `);
    check dbClient.close();
}
