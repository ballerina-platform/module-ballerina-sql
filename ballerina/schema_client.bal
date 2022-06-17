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

# Represents an SQL metadata client.
#
public type SchemaClient client object {

    remote isolated function listTables() returns string[]|Error;

    remote isolated function getTableInfo(string tableName, ColumnRetrievalOptions include = COLUMNS_ONLY) returns TableDefinition|Error;

    remote isolated function listRoutines() returns string[]|Error;

    remote isolated function getRoutineInfo(string name) returns RoutineDefinition|Error;

    # Closes the SQL metadata client.
    #
    # + return - Possible `sql:Error` when closing the client
    public isolated function close() returns Error?;
};

public enum ColumnRetrievalOptions {
    COLUMNS_ONLY,
    COLUMNS_WITH_CONSTRAINTS
}
