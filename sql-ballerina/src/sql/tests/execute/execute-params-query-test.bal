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

import ballerina/io;
import ballerina/test;

string executeParamsDb = urlPrefix + "9007/executeparams";

@test:BeforeGroups {
	value: ["execute-params"]
} 
function initExecuteParamsContainer() {
    initializeDockerContainer("sql-execute-params", "executeparams", "9007", "execute", "execute-params-test-data.sql");
}

@test:AfterGroups {
	value: ["execute-params"]
} 
function cleanExecuteParamsContainer() {
    cleanDockerContainer("sql-execute-params");
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoDataTable() {
    int rowId = 4;
    int intType = 1;
    int longType = 9223372036854774807;
    float floatType = 123.34;
    int doubleType = 2139095039;
    boolean boolType = true;
    string stringType = "Hello";
    decimal decimalType = 23.45;

    ParameterizedQuery sqlQuery =
      `INSERT INTO DataTable (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
        VALUES(${rowId}, ${intType}, ${longType}, ${floatType}, ${doubleType}, ${boolType}, ${stringType}, ${decimalType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoDataTable"]
}
function insertIntoDataTable2() {
    int rowId = 5;
    ParameterizedQuery sqlQuery = `INSERT INTO DataTable (row_id) VALUES(${rowId})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoDataTable2"]
}
function insertIntoDataTable3() {
    int rowId = 6;
    int intType = 1;
    int longType = 9223372036854774807;
    float floatType = 123.34;
    int doubleType = 2139095039;
    boolean boolType = false;
    string stringType = "1";
    decimal decimalType = 23.45;

    ParameterizedQuery sqlQuery =
      `INSERT INTO DataTable (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
        VALUES(${rowId}, ${intType}, ${longType}, ${floatType}, ${doubleType}, ${boolType}, ${stringType}, ${decimalType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoDataTable3"]
}
function insertIntoDataTable4() {
    IntegerValue rowId = new (7);
    IntegerValue intType = new (2);
    BigIntValue longType = new (9372036854774807);
    FloatValue floatType = new (124.34);
    DoubleValue doubleType = new (29095039);
    BooleanValue boolType = new (false);
    VarcharValue stringType = new ("stringvalue");
    decimal decimalVal = 25.45;
    DecimalValue decimalType = new (decimalVal);

    ParameterizedQuery sqlQuery =
      `INSERT INTO DataTable (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
        VALUES(${rowId}, ${intType}, ${longType}, ${floatType}, ${doubleType}, ${boolType}, ${stringType}, ${decimalType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoDataTable4"]
}
function deleteDataTable1() {
    int rowId = 1;
    int intType = 1;
    int longType = 9223372036854774807;
    float floatType = 123.34;
    int doubleType = 2139095039;
    boolean boolType = true;
    string stringType = "Hello";
    decimal decimalType = 23.45;

    ParameterizedQuery sqlQuery =
            `DELETE FROM DataTable where row_id=${rowId} AND int_type=${intType} AND long_type=${longType}
              AND float_type=${floatType} AND double_type=${doubleType} AND boolean_type=${boolType}
              AND string_type=${stringType} AND decimal_type=${decimalType}`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["deleteDataTable1"]
}
function deleteDataTable2() {
    int rowId = 2;
    ParameterizedQuery sqlQuery = `DELETE FROM DataTable where row_id = ${rowId}`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["deleteDataTable2"]
}
function deleteDataTable3() {
    IntegerValue rowId = new (3);
    IntegerValue intType = new (1);
    BigIntValue longType = new (9372036854774807);
    FloatValue floatType = new (124.34);
    DoubleValue doubleType = new (29095039);
    BooleanValue boolType = new (false);
    VarcharValue stringType = new ("1");
    decimal decimalVal = 25.45;
    DecimalValue decimalType = new (decimalVal);

    ParameterizedQuery sqlQuery =
            `DELETE FROM DataTable where row_id=${rowId} AND int_type=${intType} AND long_type=${longType}
              AND double_type=${doubleType} AND boolean_type=${boolType}
              AND string_type=${stringType} AND decimal_type=${decimalType}`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoComplexTable() {
    record {}? value = queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BLOB_TYPE");
    int rowId = 5;
    string stringType = "very long text";
    ParameterizedQuery sqlQuery =
        `INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type, var_binary_type) VALUES (
        ${rowId}, ${binaryData}, CONVERT(${stringType}, CLOB), ${binaryData}, ${binaryData})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoComplexTable"]
}
function insertIntoComplexTable2() {
    io:ReadableByteChannel blobChannel = getBlobColumnChannel();
    io:ReadableCharacterChannel clobChannel = getClobColumnChannel();
    io:ReadableByteChannel byteChannel = getByteColumnChannel();

    BlobValue blobType = new (blobChannel);
    ClobValue clobType = new (clobChannel);
    BlobValue binaryType = new (byteChannel);
    int rowId = 6;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type, var_binary_type) VALUES (
        ${rowId}, ${blobType}, CONVERT(${clobType}, CLOB), ${binaryType}, ${binaryType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoComplexTable2"]
}
function insertIntoComplexTable3() {
    int rowId = 7;
    var nilType = ();
    ParameterizedQuery sqlQuery =
            `INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type, var_binary_type) VALUES (
            ${rowId}, ${nilType}, CONVERT(${nilType}, CLOB), ${nilType}, ${nilType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoComplexTable3"]
}
function deleteComplexTable() {
    record {}|error? value = queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BLOB_TYPE");

    int rowId = 2;
    ParameterizedQuery sqlQuery =
            `DELETE FROM ComplexTypes where row_id = ${rowId} AND blob_type= ${binaryData}`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["deleteComplexTable"]
}
function deleteComplexTable2() {
    BlobValue blobType = new ();
    ClobValue clobType = new ();
    BinaryValue binaryType = new ();
    VarBinaryValue varBinaryType = new ();

    int rowId = 4;
    ParameterizedQuery sqlQuery =
            `DELETE FROM ComplexTypes where row_id = ${rowId} AND blob_type= ${blobType} AND clob_type=${clobType}`;
    validateResult(executeQueryMockClient(sqlQuery), 0);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoNumericTable() {
    BitValue bitType = new (1);
    int rowId = 3;
    int intType = 2147483647;
    int bigIntType = 9223372036854774807;
    int smallIntType = 32767;
    int tinyIntType = 127;
    decimal decimalType = 1234.567;

    ParameterizedQuery sqlQuery =
        `INSERT INTO NumericTypes (int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type,
        numeric_type, float_type, real_type) VALUES(${intType},${bigIntType},${smallIntType},${tinyIntType},
        ${bitType},${decimalType},${decimalType},${decimalType},${decimalType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1, 2);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoNumericTable"]
}
function insertIntoNumericTable2() {
    int rowId = 4;
    var nilType = ();
    ParameterizedQuery sqlQuery =
            `INSERT INTO NumericTypes (int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type,
            numeric_type, float_type, real_type) VALUES(${nilType},${nilType},${nilType},${nilType},
            ${nilType},${nilType},${nilType},${nilType},${nilType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1, 2);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoNumericTable2"]
}
function insertIntoNumericTable3() {
    IntegerValue id = new (5);
    IntegerValue intType = new (2147483647);
    BigIntValue bigIntType = new (9223372036854774807);
    SmallIntValue smallIntType = new (32767);
    SmallIntValue tinyIntType = new (127);
    BitValue bitType = new (1);
    decimal decimalVal = 1234.567;
    DecimalValue decimalType = new (decimalVal);
    NumericValue numbericType = new (1234.567);
    FloatValue floatType = new (1234.567);
    RealValue realType = new (1234.567);

    ParameterizedQuery sqlQuery =
        `INSERT INTO NumericTypes (int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type,
        numeric_type, float_type, real_type) VALUES(${intType},${bigIntType},${smallIntType},${tinyIntType},
        ${bitType},${decimalType},${numbericType},${floatType},${realType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1, 2);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoDateTimeTable() {
    int rowId = 2;
    string dateType = "2017-02-03";
    string timeType = "11:35:45";
    string dateTimeType = "2017-02-03 11:53:00";
    string timeStampType = "2017-02-03 11:53:00";

    ParameterizedQuery sqlQuery =
        `INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type)
        VALUES(${rowId}, ${dateType}, ${timeType}, ${dateTimeType}, ${timeStampType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoDateTimeTable"]
}
function insertIntoDateTimeTable2() {
    DateValue dateVal = new ("2017-02-03");
    TimeValue timeVal = new ("11:35:45");
    DateTimeValue dateTimeVal =  new ("2017-02-03 11:53:00");
    TimestampValue timestampVal = new ("2017-02-03 11:53:00");
    int rowId = 3;

    ParameterizedQuery sqlQuery =
            `INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type)
            VALUES(${rowId}, ${dateVal}, ${timeVal}, ${dateTimeVal}, ${timestampVal})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoDateTimeTable2"]
}
function insertIntoDateTimeTable3() {
    DateValue dateVal = new ();
    TimeValue timeVal = new ();
    DateTimeValue dateTimeVal =  new ();
    TimestampValue timestampVal = new ();
    int rowId = 4;

    ParameterizedQuery sqlQuery =
                `INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type)
                VALUES(${rowId}, ${dateVal}, ${timeVal}, ${dateTimeVal}, ${timestampVal})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoDateTimeTable3"]
}
function insertIntoDateTimeTable4() {
    int rowId = 5;
    var nilType = ();

    ParameterizedQuery sqlQuery =
            `INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type)
            VALUES(${rowId}, ${nilType}, ${nilType}, ${nilType}, ${nilType})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable() {
    int[] dataint = [1, 2, 3];
    int[] datalong = [100000000, 200000000, 300000000];
    float[] datafloat = [245.23, 5559.49, 8796.123];
    float[] datadouble = [245.23, 5559.49, 8796.123];
    decimal[] datadecimal = [245, 5559, 8796];
    string[] datastring = ["Hello", "Ballerina"];
    boolean[] databoolean = [true, false, true];

    record {}? value = queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[][] dataBlob = [<byte[]>getUntaintedData(value, "BLOB_TYPE")];

    ArrayValue paraInt = new (dataint);
    ArrayValue paraLong = new (datalong);
    ArrayValue paraFloat = new (datafloat);
    ArrayValue paraDecimal = new (datadecimal);
    ArrayValue paraDouble = new (datadouble);
    ArrayValue paraString = new (datastring);
    ArrayValue paraBool = new (databoolean);
    ArrayValue paraBlob = new (dataBlob);
    int rowId = 5;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array,
         string_array, blob_array) VALUES(${rowId}, ${paraInt}, ${paraLong}, ${paraFloat}, ${paraDouble}, ${paraDecimal},
         ${paraBool}, ${paraString}, ${paraBlob})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: ["insertIntoArrayTable"]
}
function insertIntoArrayTable2() {
    ArrayValue paraInt = new ();
    ArrayValue paraLong = new ();
    ArrayValue paraFloat = new ();
    ArrayValue paraDecimal = new ();
    ArrayValue paraDouble = new ();
    ArrayValue paraString = new ();
    ArrayValue paraBool = new ();
    ArrayValue paraBlob = new ();
    int rowId = 6;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array,
         string_array, blob_array) VALUES(${rowId}, ${paraInt}, ${paraLong}, ${paraFloat}, ${paraDouble}, ${paraDecimal},
         ${paraBool}, ${paraString}, ${paraBlob})`;
    validateResult(executeQueryMockClient(sqlQuery), 1);
}

function executeQueryMockClient(ParameterizedQuery sqlQuery)
returns ExecutionResult {
    MockClient dbClient = checkpanic new (url = executeParamsDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute(sqlQuery);
    checkpanic dbClient.close();
    return result;
}

function validateResult(ExecutionResult result, int rowCount, int? lastId = ()) {
    test:assertExactEquals(result.affectedRowCount, rowCount, "Affected row count is different.");

    if (lastId is ()) {
        test:assertEquals(result.lastInsertId, (), "Last Insert Id is not nil.");
    } else {
        int|string? lastInsertIdVal = result.lastInsertId;
        if (lastInsertIdVal is int) {
            test:assertTrue(lastInsertIdVal > 1, "Last Insert Id is nil.");
        } else {
            test:assertFail("The last insert id should be an integer found type '" + lastInsertIdVal.toString());
        }
    }
}
