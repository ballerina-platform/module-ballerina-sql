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

# Represents an SQL metadata client.
#
public type SchemaClient client object {

    # Retrieves all tables in the database.
    # 
    # + return - A string array containing the names of the tables or an `sql:Error`
    remote isolated function listTables() returns string[]|Error;

    # Retrieves information relevant to the provided table in the database.
    # 
    # + tableName - The name of the table
    # + include - Options on whether columnar and constraint related information should be fetched.
    #             If `NO_COLUMNS` is provided, then no information related to columns will be retrieved.
    #             If `COLUMNS_ONLY` is provided, then columnar information will be retrieved, but not constraint
    #             related information.
    #             If `COLUMNS_WITH_CONSTRAINTS` is provided, then columar information along with constraint related
    #             information will be retrieved
    # + return - An 'sql:TableDefinition' with the relevant table information or an `sql:Error`
    remote isolated function getTableInfo(string tableName, ColumnRetrievalOptions include = COLUMNS_ONLY) returns TableDefinition|Error;

    # Retrieves all routines in the database.
    # 
    # + return - A string array containing the names of the routines or an `sql:Error`
    remote isolated function listRoutines() returns string[]|Error;

    # Retrieves information relevant to the provided routine in the database.
    # 
    # + name - The name of the routine
    # + return - An 'sql:RoutineDefinition' with the relevant routine information or an `sql:Error`
    remote isolated function getRoutineInfo(string name) returns RoutineDefinition|Error;

    # Closes the SQL metadata client.
    #
    # + return - Possible `sql:Error` when closing the client
    public isolated function close() returns Error?;
};

public enum ColumnRetrievalOptions {
    NO_COLUMNS,
    COLUMNS_ONLY,
    COLUMNS_WITH_CONSTRAINTS
}
