// Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

string executeDb = urlPrefix + "9006/execute";

@test:BeforeGroups {
	value: ["execute-basic"]
} 
function initExecuteContainer() {
    initializeDockerContainer("sql-execute", "execute", "9006", "execute", "execute-test-data.sql");
}

@test:AfterGroups {
	value: ["execute-basic"]
} 
function cleanExecuteContainer() {
    cleanDockerContainer("sql-execute");
}

@test:Config {
    groups: ["execute", "execute-basic"]
}
function testCreateTable() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute("CREATE TABLE TestCreateTable(studentID int,"
        + " LastName varchar(255))");
    checkpanic dbClient.close();
    test:assertExactEquals(result.affectedRowCount, 0, "Affected row count is different.");
    test:assertExactEquals(result.lastInsertId, (), "Last Insert Id is not nil.");
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testCreateTable"]
}
function testInsertTable() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute("Insert into NumericTypes (int_type) values (20)");
    checkpanic dbClient.close();
    
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");
    var insertId = result.lastInsertId;
    if (insertId is int) {
        test:assertTrue(insertId >= 1, "Last Insert Id is nil.");
    } else {
        test:assertFail("Insert Id should be an integer.");
    }
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertTable"]
}
function testInsertTableWithoutGeneratedKeys() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute("Insert into StringTypes (id, varchar_type)"
        + " values (20, 'test')");
    checkpanic dbClient.close();
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");
    test:assertEquals(result.lastInsertId, (), "Last Insert Id is nil.");
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertTableWithoutGeneratedKeys"]
}
function testInsertTableWithGeneratedKeys() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute("insert into NumericTypes (int_type) values (21)");
    checkpanic dbClient.close();
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");
    var insertId = result.lastInsertId;
    if (insertId is int) {
        test:assertTrue(insertId >= 1, "Last Insert Id is nil.");
    } else {
        test:assertFail("Insert Id should be an integer.");
    }
}

type NumericType record {
    int id;
    int? int_type;
    int? bigint_type;
    int? smallint_type;
    int? tinyint_type;
    boolean? bit_type;
    decimal? decimal_type;
    decimal? numeric_type;
    float? float_type;
    float? real_type;
};

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertTableWithGeneratedKeys"]
}
function testInsertAndSelectTableWithGeneratedKeys() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute("insert into NumericTypes (int_type) values (31)");

    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");
    
    string|int? insertedId = result.lastInsertId;
    if (insertedId is int) {
        string query = string `SELECT * from NumericTypes where id = ${insertedId}`;
        stream<record{}, error> queryResult = dbClient->query(query, NumericType);

        stream<NumericType, Error> streamData = <stream<NumericType, Error>>queryResult;
        record {|NumericType value;|}? data = checkpanic streamData.next();
        checkpanic streamData.close();
        test:assertNotExactEquals(data?.value, (), "Incorrect InsetId returned.");
    } else {
        test:assertFail("Insert Id should be an integer.");
    }
    checkpanic dbClient.close();
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertAndSelectTableWithGeneratedKeys"]
}
function testInsertWithAllNilAndSelectTableWithGeneratedKeys() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute("Insert into NumericTypes (int_type, bigint_type, "
        + "smallint_type, tinyint_type, bit_type, decimal_type, numeric_type, float_type, real_type) "
        + "values (null,null,null,null,null,null,null,null,null)");

    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    string|int? insertedId = result.lastInsertId;
    if (insertedId is int) {
        string query = string `SELECT * from NumericTypes where id = ${insertedId}`;
        stream<record{}, error> queryResult = dbClient->query(query, NumericType);

        stream<NumericType, Error> streamData = <stream<NumericType, Error>>queryResult;
        record {|NumericType value;|}? data = checkpanic streamData.next();
        checkpanic streamData.close();
        test:assertNotExactEquals(data?.value, (), "Incorrect InsetId returned.");
    } else {
        test:assertFail("Insert Id should be an integer.");
    }
}

type StringData record {
    int id;
    string varchar_type;
    string charmax_type;
    string char_type;
    string charactermax_type;
    string character_type;
    string nvarcharmax_type;
    string longvarchar_type;
    string clob_type;
};

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertWithAllNilAndSelectTableWithGeneratedKeys"]
}
function testInsertWithStringAndSelectTable() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    string intIDVal = "25";
    string insertQuery = "Insert into StringTypes (id, varchar_type, charmax_type, char_type, charactermax_type, "
        + "character_type, nvarcharmax_type, longvarchar_type, clob_type) values ("
        + intIDVal + ",'str1','str2','s','str4','s','str6','str7','str8')";
    ExecutionResult result = checkpanic dbClient->execute(insertQuery);
    
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    StringData? insertedData = ();
    string query = string `SELECT * from StringTypes where id = ${intIDVal}`;
    stream<record{}, error> queryResult = dbClient->query(query, StringData);
    stream<StringData, Error> streamData = <stream<StringData, Error>>queryResult;
    record {|StringData value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();

    StringData expectedInsertRow = {
        id: 25,
        varchar_type: "str1",
        charmax_type: "str2      ",
        char_type: "s",
        charactermax_type: "str4      ",
        character_type: "s",
        nvarcharmax_type: "str6",
        longvarchar_type: "str7",
        clob_type: "str8"
    };
    test:assertEquals(data?.value, expectedInsertRow, "Incorrect InsetId returned.");

    checkpanic dbClient.close();
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertWithStringAndSelectTable"]
}
function testInsertWithEmptyStringAndSelectTable() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    string intIDVal = "35";
    string insertQuery = "Insert into StringTypes (id, varchar_type, charmax_type, char_type, charactermax_type,"
        + " character_type, nvarcharmax_type, longvarchar_type, clob_type) values (" + intIDVal +
        ",'','','','','','','','')";
    ExecutionResult result = checkpanic dbClient->execute(insertQuery);
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    string query = string `SELECT * from StringTypes where id = ${intIDVal}`;
    stream<record{}, error> queryResult = dbClient->query(query, StringData);
    stream<StringData, Error> streamData = <stream<StringData, Error>>queryResult;
    record {|StringData value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();

    StringData expectedInsertRow = {
        id: 35,
        varchar_type: "",
        charmax_type: "          ",
        char_type: " ",
        charactermax_type: "          ",
        character_type: " ",
        nvarcharmax_type: "",
        longvarchar_type: "",
        clob_type: ""
    };
    test:assertEquals(data?.value, expectedInsertRow, "Incorrect InsetId returned.");

    checkpanic dbClient.close();
}

type StringNilData record {
    int id;
    string? varchar_type;
    string? charmax_type;
    string? char_type;
    string? charactermax_type;
    string? character_type;
    string? nvarcharmax_type;
    string? longvarchar_type;
    string? clob_type;
};

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertWithEmptyStringAndSelectTable"]
}
function testInsertWithNilStringAndSelectTable() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    string intIDVal = "45";
    string test = "Insert" + intIDVal;
    string insertQuery = "Insert into StringTypes (id, varchar_type, charmax_type, char_type, charactermax_type,"
        + " character_type, nvarcharmax_type, longvarchar_type, clob_type) values ("
        + intIDVal + ",null,null,null,null,null,null,null,null)";
    ExecutionResult result = checkpanic dbClient->execute(insertQuery);
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    string query = string `SELECT * from StringTypes where id = ${intIDVal}`;
    stream<record{}, error> queryResult = dbClient->query(query, StringNilData);
    stream<StringNilData, Error> streamData = <stream<StringNilData, Error>>queryResult;
    record {|StringNilData value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();

    StringNilData expectedInsertRow = {
        id: 45,
        varchar_type: (),
        charmax_type: (),
        char_type: (),
        charactermax_type: (),
        character_type: (),
        nvarcharmax_type: (),
        longvarchar_type: (),
        clob_type: ()
    };
    test:assertEquals(data?.value, expectedInsertRow, "Incorrect InsetId returned.");
    checkpanic dbClient.close();
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertWithNilStringAndSelectTable"]
}
function testInsertTableWithDatabaseError() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult|Error result = dbClient->execute("Insert into NumericTypesNonExistTable (int_type) values (20)");

    if (result is DatabaseError) {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: Insert into NumericTypesNonExistTable " + 
                        "(int_type) values (20). user lacks privilege or object not found: NUMERICTYPESNONEXISTTABLE in " + 
                        "statement [Insert into NumericTypesNonExistTable (int_type) values (20)]."));
        DatabaseErrorDetail errorDetails = result.detail();
        test:assertEquals(errorDetails.errorCode, -5501, "SQL Error code does not match");
        test:assertEquals(errorDetails.sqlState, "42501", "SQL Error state does not match");
    } else {
        test:assertFail("Database Error expected.");
    }

    checkpanic dbClient.close();
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertTableWithDatabaseError"]
}
function testInsertTableWithDataTypeError() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult|Error result = dbClient->execute("Insert into NumericTypes (int_type) values"
        + " ('This is wrong type')");

    if (result is DatabaseError) {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: Insert into NumericTypes " + 
                    "(int_type) values ('This is wrong type'). data exception: invalid character value for cast."),
                    "Error message does not match, actual :'" + result.message() + "'");
        DatabaseErrorDetail errorDetails = result.detail();
        test:assertEquals(errorDetails.errorCode, -3438, "SQL Error code does not match");
        test:assertEquals(errorDetails.sqlState, "22018", "SQL Error state does not match");
    } else {
        test:assertFail("Database Error expected.");
    }

    checkpanic dbClient.close();
}

type ResultCount record {
    int countVal;
};

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: ["testInsertTableWithDataTypeError"]
}
function testUpdateData() {
    MockClient dbClient = checkpanic new (url = executeDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute("Update NumericTypes set int_type = 11 where int_type = 10");
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");
    
    stream<record{}, error> queryResult = dbClient->query("SELECT count(*) as countval from NumericTypes"
        + " where int_type = 11", ResultCount);
    stream<ResultCount, Error> streamData = <stream<ResultCount, Error>>queryResult;
    record {|ResultCount value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();
    test:assertEquals(data?.value?.countVal, 1, "Update command was not successful.");

    checkpanic dbClient.close();
}
