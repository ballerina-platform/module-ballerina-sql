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

import ballerina/lang.'string as strings;
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

@test:Config {
    groups: ["error"]
}
function TestAuthenticationError() {
    MockClient|error dbClient = new (url = errorDB, user = user, password = "password");
    test:assertTrue(dbClient is ApplicationError);
    ApplicationError sqlError = <ApplicationError> dbClient;
    test:assertEquals(sqlError.message(), "Error in SQL connector configuration: Failed to initialize pool:" +
            " invalid authorization specification Caused by :invalid authorization specification Caused" +
            " by :invalid authorization specification", sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestConnectionError() {
    MockClient|error dbClient = new (url = "jdbc:hsqldb:hsql://localst:9093/error", user = user, password = "password");
    test:assertTrue(dbClient is ApplicationError);
    ApplicationError sqlError = <ApplicationError> dbClient;
    test:assertEquals(sqlError.message(), "Error in SQL connector configuration: Failed to initialize pool: " +
                "localst Caused by :localst Caused by :localst Caused by :localst", sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestInvalidDB() {
    MockClient|error dbClient = new (url = "jdbc:hsqldb:hsql://localhost:9093/err", user = user, password = "password");
    test:assertTrue(dbClient is ApplicationError);
    ApplicationError sqlError = <ApplicationError> dbClient;
    test:assertEquals(sqlError.message(), "Error in SQL connector configuration: Failed to initialize pool: " +
            "Connection refused (Connection refused) Caused by :Connection refused (Connection refused) Caused by " +
            ":Connection refused (Connection refused) Caused by :Connection refused (Connection refused)",
            sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestConnectionClose() returns error? {
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = 1`;
    MockClient mockClient = check getMockClient(errorDB);
    check mockClient.close();
    string|Error stringVal = mockClient->queryRow(sqlQuery);
    test:assertTrue(stringVal is ApplicationError);
    ApplicationError sqlError = <ApplicationError> stringVal;
    test:assertEquals(sqlError.message(), "SQL Client is already closed, hence further operations are not allowed",
                sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestInvalidTableName() returns error? {
    ParameterizedQuery sqlQuery = `SELECT string_type from Data WHERE row_id = 1`;
    MockClient mockClient = check getMockClient(errorDB);
    string|Error stringVal = mockClient->queryRow(sqlQuery);
    check mockClient.close();
    test:assertTrue(stringVal is DatabaseError);
    error sqlError = <error> stringVal;
     test:assertEquals(sqlError.message(), "Error while executing SQL query: SELECT string_type from Data WHERE " +
             "row_id = 1. user lacks privilege or object not found: DATA in statement [SELECT string_type from Data " +
             "WHERE row_id = 1].", sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestInvalidFieldName() returns error? {
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE id = 1`;
    MockClient mockClient = check getMockClient(errorDB);
    string|Error stringVal = mockClient->queryRow(sqlQuery);
    check mockClient.close();
    test:assertTrue(stringVal is DatabaseError);
    DatabaseError sqlError = <DatabaseError> stringVal;
    test:assertEquals(sqlError.message(), "Error while executing SQL query: SELECT string_type from DataTable WHERE " +
            "id = 1. user lacks privilege or object not found: ID in statement [SELECT string_type from DataTable " +
            "WHERE id = 1].", sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestInvalidColumnType() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    ExecutionResult|error result = mockClient->execute(`CREATE TABLE TestCreateTable(studentID Point, LastName string)`);
    check mockClient.close();
    DatabaseError sqlError = <DatabaseError> result;
    test:assertEquals(sqlError.message(), "Error while executing SQL query: CREATE TABLE " +
            "TestCreateTable(studentID Point, LastName string). type not found or user lacks privilege: POINT " +
            "in statement [CREATE TABLE TestCreateTable(studentID Point, LastName string)].", sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestNullValue() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    _ = check mockClient->execute(`CREATE TABLE TestCreateTable(studentID int not null, LastName VARCHAR(50))`);
    ParameterizedQuery insertQuery = `Insert into TestCreateTable (studentID, LastName) values (null,'asha')`;
    ExecutionResult|error insertResult = mockClient->execute(insertQuery);
    check mockClient.close();
    test:assertTrue(insertResult is DatabaseError);
    DatabaseError sqlError = <DatabaseError> insertResult;
    test:assertTrue(strings:includes(sqlError.message(), "Error while executing SQL query: Insert into TestCreateTable " +
                "(studentID, LastName) values (null,'asha'). integrity constraint violation: NOT NULL check " +
                "constraint;"), sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestNoDataRead() returns error? {
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = 5`;
    MockClient mockClient = check getMockClient(errorDB);
    record {}|error queryResult = mockClient->queryRow(sqlQuery);
    test:assertTrue(queryResult is NoRowsError);
    NoRowsError sqlError = <NoRowsError> queryResult;
    test:assertEquals(sqlError.message(), "Query did not retrieve any rows.", sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestUnsupportedTypeValue() returns error? {
    VarcharValue value = new("hi");
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = ${value}`;
    MockClient mockClient = check getMockClient(errorDB);
    string|Error stringVal = mockClient->queryRow(sqlQuery);
    check mockClient.close();
    test:assertTrue(stringVal is DatabaseError);
    DatabaseError sqlError = <DatabaseError> stringVal;
    test:assertEquals(sqlError.message(), "Error while executing SQL query: SELECT string_type from DataTable " +
            "WHERE row_id =  ? . data exception: invalid character value for cast.", sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestConversionError() returns error? {
    DateValue value = new("hi");
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = ${value}`;
    MockClient mockClient = check getMockClient(errorDB);
    string|Error stringVal = mockClient->queryRow(sqlQuery);
    check mockClient.close();
    test:assertTrue(stringVal is ConversionError);
    ConversionError sqlError = <ConversionError> stringVal;
    test:assertEquals(sqlError.message(), "Unsupported value: hi for Date Value", sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestConversionError1() returns error? {
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = 1`;
    MockClient mockClient = check getMockClient(errorDB);
    json|error queryResult = mockClient->queryRow(sqlQuery);
    test:assertTrue(queryResult is ConversionError);
    ConversionError sqlError = <ConversionError> queryResult;
    test:assertTrue(strings:includes(sqlError.message(), "Retrieved column 1 result '{\"\"q}' could not be converted"),
                sqlError.message());
}

type data record {|
    int row_id;
    int string_type;
|};

@test:Config {
    groups: ["error"]
}
function TestTypeMismatchError() returns error? {
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = 1`;
    MockClient mockClient = check getMockClient(errorDB);
    data|error queryResult = mockClient->queryRow(sqlQuery);
    test:assertTrue(queryResult is TypeMismatchError);
    TypeMismatchError sqlError = <TypeMismatchError> queryResult;
    test:assertTrue(strings:includes(sqlError.message(), "The field 'string_type' of type int cannot be mapped to the" +
            " column 'STRING_TYPE' of SQL type 'VARCHAR'"), sqlError.message());
}

type stringValue record {|
    int row_id1;
    string string_type1;
|};

@test:Config {
    groups: ["error"]
}
function TestFieldMismatchError() returns error? {
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = 1`;
    MockClient mockClient = check getMockClient(errorDB);
    stringValue|error queryResult = mockClient->queryRow(sqlQuery);
    test:assertTrue(queryResult is FieldMismatchError);
    FieldMismatchError sqlError = <FieldMismatchError> queryResult;
    test:assertTrue(strings:includes(sqlError.message(), "No mapping field found for SQL table " +
            "column 'STRING_TYPE' in the record type 'stringValue'"), sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestUndefinedColumnValue() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    ParameterizedQuery insertQuery = `Insert into DataTable (row_id) values ('xyx')`;
    ExecutionResult|error insertResult = mockClient->execute(insertQuery);
    check mockClient.close();
    test:assertTrue(insertResult is DatabaseError);
    DatabaseError sqlError = <DatabaseError> insertResult;
    test:assertTrue(strings:includes(sqlError.message(), "Error while executing SQL query: Insert into DataTable " +
              "(row_id) values ('xyx'). data exception: invalid character value for cast."), sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestUnknownColumn() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    ParameterizedQuery insertQuery = `Insert into DataTable (id) values (4)`;
    ExecutionResult|error insertResult = mockClient->execute(insertQuery);
    check mockClient.close();
    test:assertTrue(insertResult is DatabaseError);
    DatabaseError sqlError = <DatabaseError> insertResult;
    test:assertTrue(strings:includes(sqlError.message(), "Error while executing SQL query: Insert into DataTable " +
             "(id) values (4). user lacks privilege or object not found: ID in statement [Insert into DataTable " +
             "(id) values (4)]"), sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestDuplicateKey() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    ParameterizedQuery insertQuery = `CREATE TABLE IF NOT EXISTS Customers(id INT IDENTITY, PRIMARY KEY (id))`;
    _ = check mockClient->execute(insertQuery);
    insertQuery = `Insert into Customers (id) values (1)`;
    ExecutionResult|error insertResult = mockClient->execute(insertQuery);
    insertResult = mockClient->execute(insertQuery);
    check mockClient.close();
    test:assertTrue(insertResult is DatabaseError);
    DatabaseError sqlError = <DatabaseError> insertResult;
    test:assertTrue(strings:includes(sqlError.message(), "Error while executing SQL query: Insert into Customers " +
                "(id) values (1). integrity constraint violation: unique constraint or index violation;"),
                 sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function TestDeleteRowNotFound() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    ParameterizedQuery insertQuery = `DELETE FROM DataTable where id=5`;
    ExecutionResult|error insertResult = mockClient->execute(insertQuery);
    check mockClient.close();
    test:assertTrue(insertResult is DatabaseError);
    DatabaseError sqlError = <DatabaseError> insertResult;
    test:assertTrue(strings:includes(sqlError.message(), "Error while executing SQL query: DELETE FROM DataTable" +
             " where id=5. user lacks privilege or object not found: ID in statement [DELETE FROM DataTable " +
             "where id=5]"), sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function testCreateProceduresWithMissingParams() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    _ = check mockClient->execute(`CREATE PROCEDURE InsertStudent (IN pName VARCHAR(255),
                                 IN pAge INT) MODIFIES SQL DATA INSERT INTO DataTable(row_id,
                                 string_type) VALUES (pAge, pName);`);
    ProcedureCallResult|error result = mockClient->call(`call InsertStudent(1);`);
    check mockClient.close();
    DatabaseError sqlError = <DatabaseError> result;
    test:assertEquals(sqlError.message(), "Error while executing SQL query: call InsertStudent(1);. " +
                "user lacks privilege or object not found in statement [call InsertStudent(1);].",
                sqlError.message());
}

@test:Config {
    groups: ["error"],
    dependsOn: [testCreateProcedures1]
}
function testCreateProceduresWithParameterTypeMismatch() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    ProcedureCallResult|error result = mockClient->call(`call InsertStudent(1, 1);`);
    check mockClient.close();
    DatabaseError sqlError = <DatabaseError> result;
    test:assertEquals(sqlError.message(), "Error while executing SQL query: call InsertStudent(1, 1);. " +
                "incompatible data type in conversion in statement [call InsertStudent(1, 1);].",
                sqlError.message());
}

@test:Config {
    groups: ["error"]
}
function testCreateProceduresWithInvalidArgument() returns error? {
    MockClient mockClient = check getMockClient(errorDB);
    ExecutionResult|error result = mockClient->execute(
            `CREATE PROCEDURE InsertData (IN_OUT pName VARCHAR(255) MODIFIES SQL DATA INSERT INTO DataTable(row_id) VALUES (pAge);`);
    check mockClient.close();
    DatabaseError sqlError = <DatabaseError> result;
    test:assertEquals(sqlError.message(), "Error while executing SQL query: CREATE PROCEDURE InsertData " +
                "(IN_OUT pName VARCHAR(255) MODIFIES SQL DATA INSERT INTO DataTable(row_id) VALUES (pAge);. type " +
                "not found or user lacks privilege: PNAME in statement [CREATE PROCEDURE InsertData (IN_OUT pName " +
                "VARCHAR(255) MODIFIES SQL DATA INSERT INTO DataTable(row_id) VALUES (pAge);].",
                sqlError.message());
}
