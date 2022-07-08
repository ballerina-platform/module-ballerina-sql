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
        stream<record {}, Error?> tablesStream = self.dbClient->query(
            `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ${self.database}`
        );

        string[]? tables = check from record {} 'table in tablesStream
                                    select <string>'table["TABLE_NAME"];
        if tables is () {
            return [];
        } else {
            return tables;
        }
    }

    isolated remote function getTableInfo(string tableName, ColumnRetrievalOptions include = COLUMNS_ONLY) returns TableDefinition|Error {
        record {}|Error 'table = self.dbClient->queryRow(`
            SELECT TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ${tableName} AND TABLE_SCHEMA = ${self.database}
        `);

        if 'table is NoRowsError {
            return <NoRowsError>error("The table '" + tableName + "' does not exist or the user does not have the required privilege level to view it.");
        } if 'table is Error {
            return 'table;
        } else {
            TableDefinition result = {
                name: tableName,
                'type: <TableType>'table["TABLE_TYPE"]
            };

            if !(include is NO_COLUMNS) {
                result.columns = check self.getColumns(tableName);

                if include is COLUMNS_WITH_CONSTRAINTS {
                    map<ReferentialConstraint[]> refConstraintsMap = check self.getReferentialConstraints(tableName);
                    map<CheckConstraint[]> checkConstraintsMap = check self.getCheckConstraints(tableName);

                    _ = checkpanic from ColumnDefinition column in <ColumnDefinition[]>result.columns
                        do {
                            ReferentialConstraint[]? refConstraints = refConstraintsMap[column.name];
                            if !(refConstraints is ()) && refConstraints.length() != 0 {
                                column.referentialConstraints = refConstraints;
                            }

                            CheckConstraint[]? checkConstraints = checkConstraintsMap[column.name];
                            if !(checkConstraints is ()) && checkConstraints.length() != 0 {
                                column.checkConstraints = checkConstraints;
                            }
                        };     
                }
            }
            return result; 
        }
    }

    isolated remote function listRoutines() returns string[]|Error {
        stream<record {}, Error?> routinesStream = self.dbClient->query(
            `SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = ${self.database}`
        );
        string[]? routines = check from record {} routine in routinesStream
                                    select <string>routine["ROUTINE_NAME"];
        if routines is () {
            return [];
        } else {
            return routines;
        }
    }

    isolated remote function getRoutineInfo(string name) returns RoutineDefinition|Error {
        record {}|Error routine = self.dbClient->queryRow(`
            SELECT ROUTINE_TYPE, DATA_TYPE
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_SCHEMA = ${self.database} AND ROUTINE_NAME = ${name}
        `);

        if routine is NoRowsError {
            return <NoRowsError>error("The routine '" + name + "' does not exist or the user does not have the required privilege level to view it.");
        } if routine is Error {
            return routine;
        } else {
            RoutineDefinition result = {
                name: name,
                'type: <RoutineType>routine["ROUTINE_TYPE"],
                returnType: routine["DATA_TYPE"] is string ? <string>routine["DATA_TYPE"] : (),
                parameters: check self.getRoutineParameters(name)
            };
            return result;
        }
    }

    public isolated function close() returns Error? {
        _ = check self.dbClient.close();
    }

    private isolated function getColumns(string tableName) returns ColumnDefinition[]|Error {
        ColumnDefinition[] columns = [];
        stream<record {}, Error?> columnStream = self.dbClient->query(`
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ${self.database} AND TABLE_NAME = ${tableName}
        `);

        error? e = from record {} retrievedColumn in columnStream
            do {
                ColumnDefinition column = {
                    name: <string>retrievedColumn["COLUMN_NAME"],
                    'type: <string>retrievedColumn["DATA_TYPE"],
                    defaultValue: retrievedColumn["COLUMN_DEFAULT"],
                    nullable: (<string>retrievedColumn["IS_NULLABLE"]) == "YES" ? true : false
                };
                columns.push(column);
            };
        if e is error {
            return <DataError>error(e.message());
        }
        return columns;
    }

    private isolated function getReferentialConstraints(string tableName) returns map<ReferentialConstraint[]>|Error {
        map<ReferentialConstraint[]> refConstraintsMap = {};

        stream<record {}, Error?> referentialConstraintStream = self.dbClient->query(`
            SELECT 
                KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME,
                KCU1.COLUMN_NAME AS FK_COLUMN_NAME,
                KCU2.TABLE_NAME AS UQ_TABLE_NAME,
                KCU2.COLUMN_NAME AS UQ_COLUMN_NAME,
                RC.UPDATE_RULE AS UPDATE_RULE,
                RC.DELETE_RULE AS DELETE_RULE
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1
                ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG 
                AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA
                AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2
                ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG 
                AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA
                AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
            WHERE 
                RC.CONSTRAINT_SCHEMA = ${self.database}
                AND KCU1.TABLE_NAME = ${tableName}
        `);

        error? e = from record {} refConstraints in referentialConstraintStream
            do {
                ReferentialConstraint refConstraint = {
                    name: <string>refConstraints["FK_CONSTRAINT_NAME"],
                    tableName: <string>refConstraints["UQ_TABLE_NAME"],
                    columnName: <string>refConstraints["UQ_COLUMN_NAME"],
                    updateRule: <ReferentialRule>refConstraints["UPDATE_RULE"],
                    deleteRule: <ReferentialRule>refConstraints["DELETE_RULE"]
                };
                string columnName = <string>refConstraints["FK_COLUMN_NAME"];
                if refConstraintsMap[columnName] is () {
                    refConstraintsMap[columnName] = [];
                }
                refConstraintsMap.get(columnName).push(refConstraint);
            };
        if e is error {
            return <DataError>error(e.message());
        }
        return refConstraintsMap;
    }

    private isolated function getCheckConstraints(string tableName) returns map<CheckConstraint[]>|Error {
        map<CheckConstraint[]> checkConstraintsMap = {};
        stream<record {}, error?> checkConstraintStream = self.dbClient->query(`
            SELECT 
                CC.CONSTRAINT_NAME AS CONSTRAINT_NAME,
                CC.CHECK_CLAUSE AS CHECK_CLAUSE,
                CCU.COLUMN_NAME AS COLUMN_NAME
            FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS CC
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU  
                ON CC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME
            WHERE 
                CC.CONSTRAINT_SCHEMA = ${self.database}
                AND CCU.TABLE_NAME = ${tableName}
        `);

        error? e = from record {} retrievedCheckConstraint in checkConstraintStream
            do {
                if !(<string>retrievedCheckConstraint["CHECK_CLAUSE"]).endsWith("IS NOT NULL") {
                    CheckConstraint checkConstraint = {
                        name: <string>retrievedCheckConstraint["CONSTRAINT_NAME"],
                        clause: <string>retrievedCheckConstraint["CHECK_CLAUSE"]
                    };
                    string columnName = <string>retrievedCheckConstraint["COLUMN_NAME"];
                    if checkConstraintsMap[columnName] is () {
                        checkConstraintsMap[columnName] = [];
                    }
                    checkConstraintsMap.get(columnName).push(checkConstraint);
                }
            };
        if e is error {
            return <DataError>error(e.message());
        }
        return checkConstraintsMap;
    }

    private isolated function getRoutineParameters(string name) returns ParameterDefinition[]|Error {
        ParameterDefinition[] parameters = [];

        stream<record {}, error?> parametersStream = self.dbClient->query(`
            SELECT
                P.PARAMETER_MODE AS PARAMETER_MODE,
                P.PARAMETER_NAME AS PARAMETER_NAME,
                P.DATA_TYPE AS DATA_TYPE
            FROM INFORMATION_SCHEMA.PARAMETERS AS P
            JOIN INFORMATION_SCHEMA.ROUTINES AS R
            ON P.SPECIFIC_NAME = R.SPECIFIC_NAME
            WHERE 
                P.SPECIFIC_SCHEMA = ${self.database} AND
                R.ROUTINE_NAME = ${name}
        `);

        error? e = from record {} retrievedParameter in parametersStream
            do {
                ParameterDefinition 'parameter = {
                    mode: <ParameterMode>retrievedParameter["PARAMETER_MODE"],
                    name: <string>retrievedParameter["PARAMETER_NAME"],
                    'type: <string>retrievedParameter["DATA_TYPE"]
                };
                parameters.push('parameter);
            };
        if e is error {
            return <DataError>error(e.message());
        }
        return parameters;
    }
}
