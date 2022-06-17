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

# Represents a mock database schema client.
isolated client class MockSchemaClient {
    *SchemaClient;

    private final MockClient dbClient;
    private final string database;

    public function init(string url, string user, string password, string database) returns Error? {
        self.database = database;
        self.dbClient = check new(url, user, password);
    }

    isolated remote function listTables() returns string[]|Error {
        stream<record {string name;}, error?> tablesStream = self.dbClient->query(
            `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ${self.database}`
        );
        string[] tables = [];
        error? e = check from record {string name;} 'table in tablesStream
            do {
                tables.push('table.name);
            };
        return tables;
    }

    isolated remote function getTableInfo(string tableName, ColumnRetrievalOptions include = COLUMNS_ONLY) 
    returns TableDefinition|Error {
        return error("sdf");
    }

    isolated remote function listRoutines() returns string[]|Error {
        return [];
    }

    isolated remote function getRoutineInfo(string name) returns RoutineDefinition|Error {
        return error("sdf");
    }

    public isolated function close() returns Error? {
        _ = check self.dbClient.close();
    }

}