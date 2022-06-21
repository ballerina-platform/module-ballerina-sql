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
        stream<record {string TABLE_NAME;}, error?> tablesStream = self.dbClient->query(
            `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ${self.database}`
        );
        string[] tables = [];

        error? e = from record {string TABLE_NAME;} 'table in tablesStream
            do {
                tables.push('table.TABLE_NAME);
            };
        if e is error {
            return <InsufficientPrivilegesError>error("dsf");
        }

        return tables;
    }

    isolated remote function getTableInfo(string tableName, ColumnRetrievalOptions include = COLUMNS_ONLY) 
    returns TableDefinition|Error {
        record {string TABLE_TYPE;} 'table = check self.dbClient->queryRow(
            `SELECT TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ${tableName} AND TABLE_SCHEMA = ${self.database}`
        );
        TableDefinition result = {
            name: tableName,
            'type: 'table.TABLE_TYPE is "BASE TABLE" ? BASE_TABLE : VIEW
        };

        if include is COLUMNS_ONLY {
            ColumnDefinition[] columns = [];
            stream<record {
                string COLUMN_NAME;
                string DATA_TYPE;
                string COLUMN_DEFAULT;
                string IS_NULLABLE;
            }, error?> columnStream = self.dbClient->query(
                `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ${self.database} AND TABLE_NAME = ${tableName}`
            );
            error? e = from record {
                string COLUMN_NAME;
                string DATA_TYPE;
                string COLUMN_DEFAULT;
                string IS_NULLABLE;
            } column in columnStream
                do {
                    ColumnDefinition column2 = {
                        name: column.COLUMN_NAME,
                        'type: column.DATA_TYPE,
                        defaultValue: column.COLUMN_DEFAULT,
                        nullable: false
                    };
                    columns.push(column2);
                };
            if e is error {
                return <InsufficientPrivilegesError>error("sfd");
            }
            result.columns = columns;
        }
        return result;   
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