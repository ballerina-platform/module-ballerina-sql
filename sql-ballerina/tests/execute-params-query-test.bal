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
import ballerina/lang.'string as strings;
import ballerina/test;
import ballerina/time;

string executeParamsDb = urlPrefix + "9007/executeparams";

@test:BeforeGroups {
	value: ["execute-params"]
} 
function initExecuteParamsContainer() returns error? {
    check initializeDockerContainer("sql-execute-params", "executeparams", "9007", "execute", "execute-params-test-data.sql");
}

@test:AfterGroups {
	value: ["execute-params"]
} 
function cleanExecuteParamsContainer() returns error? {
    check cleanDockerContainer("sql-execute-params");
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoDataTable() returns error? {
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
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDataTable]
}
function insertIntoDataTable2() returns error? {
    int rowId = 5;
    ParameterizedQuery sqlQuery = `INSERT INTO DataTable (row_id) VALUES(${rowId})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDataTable2]
}
function insertIntoDataTable3() returns error? {
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
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDataTable3]
}
function insertIntoDataTable4() returns error? {
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
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDataTable4]
}
function deleteDataTable1() returns error? {
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
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [deleteDataTable1]
}
function deleteDataTable2() returns error? {
    int rowId = 2;
    ParameterizedQuery sqlQuery = `DELETE FROM DataTable where row_id = ${rowId}`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [deleteDataTable2]
}
function deleteDataTable3() returns error? {
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
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoComplexTable() returns error? {
    record {}? value = check queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BLOB_TYPE");
    int rowId = 5;
    string stringType = "very long text";
    ParameterizedQuery sqlQuery =
        `INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type, var_binary_type) VALUES (
        ${rowId}, ${binaryData}, CONVERT(${stringType}, CLOB), ${binaryData}, ${binaryData})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoComplexTable]
}
function insertIntoComplexTable2() returns error? {
    io:ReadableByteChannel blobChannel = check getBlobColumnChannel();
    io:ReadableCharacterChannel clobChannel = check getClobColumnChannel();
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();

    BlobValue blobType = new (blobChannel);
    ClobValue clobType = new (clobChannel);
    BlobValue binaryType = new (byteChannel);
    int rowId = 6;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type, var_binary_type) VALUES (
        ${rowId}, ${blobType}, CONVERT(${clobType}, CLOB), ${binaryType}, ${binaryType})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoComplexTable2]
}
function insertIntoComplexTable3() returns error? {
    int rowId = 7;
    var nilType = ();
    ParameterizedQuery sqlQuery =
            `INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type, var_binary_type) VALUES (
            ${rowId}, ${nilType}, CONVERT(${nilType}, CLOB), ${nilType}, ${nilType})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoComplexTable3]
}
function deleteComplexTable() returns error? {
    record {}|error? value = check queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BLOB_TYPE");

    int rowId = 2;
    ParameterizedQuery sqlQuery =
            `DELETE FROM ComplexTypes where row_id = ${rowId} AND blob_type= ${binaryData}`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [deleteComplexTable]
}
function deleteComplexTable2() returns error? {
    BlobValue blobType = new ();
    ClobValue clobType = new ();
    BinaryValue binaryType = new ();
    VarBinaryValue varBinaryType = new ();

    int rowId = 4;
    ParameterizedQuery sqlQuery =
            `DELETE FROM ComplexTypes where row_id = ${rowId} AND blob_type= ${blobType} AND clob_type=${clobType}`;
    validateResult(check executeQueryMockClient(sqlQuery), 0);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoNumericTable() returns error? {
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
    validateResult(check executeQueryMockClient(sqlQuery), 1, 2);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoNumericTable]
}
function insertIntoNumericTable2() returns error? {
    int rowId = 4;
    var nilType = ();
    ParameterizedQuery sqlQuery =
            `INSERT INTO NumericTypes (int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type,
            numeric_type, float_type, real_type) VALUES(${nilType},${nilType},${nilType},${nilType},
            ${nilType},${nilType},${nilType},${nilType},${nilType})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1, 2);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoNumericTable2]
}
function insertIntoNumericTable3() returns error? {
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
    validateResult(check executeQueryMockClient(sqlQuery), 1, 2);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoDateTimeTable() returns error? {
    int rowId = 2;
    string dateType = "2017-02-03";
    string timeType = "11:35:45";
    string dateTimeType = "2017-02-03 11:53:00";
    string timeStampType = "2017-02-03 11:53:00";

    ParameterizedQuery sqlQuery =
        `INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type)
        VALUES(${rowId}, ${dateType}, ${timeType}, ${dateTimeType}, ${timeStampType})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDateTimeTable]
}
function insertIntoDateTimeTable2() returns error? {
    DateValue dateVal = new ("2017-02-03");
    TimeValue timeVal = new ("11:35:45");
    DateTimeValue dateTimeVal =  new ("2017-02-03 11:53:00");
    TimestampValue timestampVal = new ("2017-02-03 11:53:00");
    int rowId = 3;

    ParameterizedQuery sqlQuery =
            `INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type)
            VALUES(${rowId}, ${dateVal}, ${timeVal}, ${dateTimeVal}, ${timestampVal})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDateTimeTable2]
}
function insertIntoDateTimeTable3() returns error? {
    DateValue dateVal = new ();
    TimeValue timeVal = new ();
    DateTimeValue dateTimeVal =  new ();
    TimestampValue timestampVal = new ();
    int rowId = 4;

    ParameterizedQuery sqlQuery =
                `INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type)
                VALUES(${rowId}, ${dateVal}, ${timeVal}, ${dateTimeVal}, ${timestampVal})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDateTimeTable3]
}
function insertIntoDateTimeTable4() returns error? {
    int rowId = 5;
    var nilType = ();

    ParameterizedQuery sqlQuery =
            `INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type)
            VALUES(${rowId}, ${nilType}, ${nilType}, ${nilType}, ${nilType})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable() returns error? {
    int[] dataint = [1, 2, 3];
    int[] datalong = [100000000, 200000000, 300000000];
    float[] datafloat = [245.23, 5559.49, 8796.123];
    float[] datadouble = [245.23, 5559.49, 8796.123];
    decimal[] datadecimal = [245, 5559, 8796];
    string[] datastring = ["Hello", "Ballerina"];
    boolean[] databoolean = [true, false, true];

    record {}? value = check queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
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
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoArrayTable]
}
function insertIntoArrayTable2() returns error? {
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
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable3() returns error? {
    float float1 = 19.21;
    float float2 = 492.98;
    SmallIntValue smallintValue1 = new (1211);
    SmallIntValue smallintValue2 = new (478);
    SmallIntValue[] datasmallint = [smallintValue1, smallintValue2];
    IntegerValue integerValue1 = new (121);
    IntegerValue integerValue2 = new (498);
    IntegerValue[] dataint = [integerValue1, integerValue2];
    BigIntValue bigIntValue1 = new (121);
    BigIntValue bigIntValue2 = new (498);
    BigIntValue[] datalong = [bigIntValue1, bigIntValue2];
    FloatValue floatValue1 = new (float1);
    FloatValue floatValue2 = new (float2);
    FloatValue[] datafloat = [floatValue1, floatValue2];
    DoubleValue doubleValue1 = new (float1);
    DoubleValue doubleValue2 = new (float2);
    DoubleValue[] datadouble = [doubleValue1, doubleValue2];
    RealValue realValue1 = new (float1);
    RealValue realValue2 = new (float2);
    RealValue[] dataReal = [realValue1, realValue2];
    DecimalValue decimalValue1 = new (<decimal> 12.245);
    DecimalValue decimalValue2 = new (<decimal> 13.245);
    DecimalValue[] datadecimal = [decimalValue1, decimalValue2];
    NumericValue numericValue1 = new (float1);
    NumericValue numericValue2 = new (float2);
    NumericValue[] dataNumeric = [numericValue1, numericValue2];
    CharValue charValue1 = new ("Char value");
    CharValue charValue2 = new ("Character");
    CharValue[] dataChar = [charValue1, charValue2];
    NVarcharValue nvarcharValue1 = new ("NVarchar value");
    NVarcharValue nvarcharValue2 = new ("Varying NChar");
    NVarcharValue[] dataNVarchar = [nvarcharValue1, nvarcharValue2];
    string[] datastring = ["Hello", "Ballerina"];
    BooleanValue trueValue = new (true);
    BooleanValue falseValue = new (false);
    BooleanValue[] databoolean = [trueValue, falseValue, trueValue];
    DateValue date1 = new ("2021-12-18");
    DateValue date2 = new ("2021-12-19");
    DateValue[] dataDate = [date1, date2];
    time:TimeOfDay time = {hour: 20, minute: 8, second: 12};
    TimeValue time1 = new (time);
    TimeValue time2 = new (time);
    TimeValue[] dataTime = [time1, time2];
    time:Civil datetime = {year: 2021, month: 12, day: 18, hour: 20, minute: 8, second: 12};
    DateTimeValue datetime1 = new (datetime);
    DateTimeValue datetime2 = new (datetime);
    DateTimeValue[] dataDatetime = [datetime1, datetime2];
    time:Utc timestampUtc = [12345600, 12];
    TimestampValue timestamp1 = new (timestampUtc);
    TimestampValue timestamp2 = new (timestampUtc);
    TimestampValue[] dataTimestamp = [timestamp1, timestamp2];
    byte[] byteArray1 = [1, 2, 3];
    byte[] byteArray2 = [4, 5, 6];
    BinaryValue binary1 = new (byteArray1);
    BinaryValue binary2 = new (byteArray2);
    BinaryValue[] dataBinary = [binary1, binary2];
    VarBinaryValue varBinary1 = new (byteArray1);
    VarBinaryValue varBinary2 = new (byteArray2);
    VarBinaryValue[] dataVarBinary = [varBinary1, varBinary2];
    io:ReadableByteChannel byteChannel = check getBlobColumnChannel();
    record {}? value = check queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[][] dataBlob = [<byte[]>getUntaintedData(value, "BLOB_TYPE")];

    ArrayValue paraSmallint = new (datasmallint);
    ArrayValue paraInt = new (dataint);
    ArrayValue paraLong = new (datalong);
    ArrayValue paraFloat = new (datafloat);
    ArrayValue paraReal = new (dataReal);
    ArrayValue paraDecimal = new (datadecimal);
    ArrayValue paraNumeric = new (dataNumeric);
    ArrayValue paraDouble = new (datadouble);
    ArrayValue paraChar = new (dataChar);
    VarcharArrayValue paraVarchar = new (["Varchar value", "Varying Char"]);
    ArrayValue paraNVarchar = new (dataNVarchar);
    ArrayValue paraString = new (datastring);
    ArrayValue paraBool = new (databoolean);
    ArrayValue paraDate = new (dataDate);
    ArrayValue paraTime = new (dataTime);
    ArrayValue paraDatetime = new (dataDatetime);
    ArrayValue paraTimestamp = new (dataTimestamp);
    ArrayValue paraBinary = new (dataBinary);
    ArrayValue paraVarBinary = new (dataVarBinary);
    ArrayValue paraBlob = new (dataBlob);
    int rowId = 7;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array,
         string_array, smallint_array, numeric_array, real_array, char_array, varchar_array, nvarchar_array, date_array, time_array, datetime_array, timestamp_array, binary_array, varbinary_array, blob_array) VALUES(${rowId}, ${paraInt}, ${paraLong}, ${paraFloat}, ${paraDouble}, ${paraDecimal},
         ${paraBool}, ${paraString}, ${paraSmallint}, ${paraNumeric}, ${paraReal}, ${paraChar}, ${paraVarchar}, ${paraNVarchar}, ${paraDate}, ${paraTime}, ${paraDatetime}, ${paraTimestamp}, ${paraBinary}, ${paraVarBinary}, ${paraBlob})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable4() returns error? {
    ArrayValue paraSmallint = new ();
    ArrayValue paraInt = new ();
    ArrayValue paraLong = new ();
    ArrayValue paraFloat = new ();
    ArrayValue paraReal = new ();
    ArrayValue paraDecimal = new ();
    ArrayValue paraNumeric = new ();
    ArrayValue paraDouble = new ();
    ArrayValue paraChar = new ();
    ArrayValue paraVarchar = new ();
    ArrayValue paraNVarchar = new ();
    ArrayValue paraString = new ();
    ArrayValue paraBool = new ();
    ArrayValue paraDate = new ();
    ArrayValue paraTime = new ();
    ArrayValue paraDatetime = new ();
    ArrayValue paraTimestamp = new ();
    ArrayValue paraBinary = new ();
    ArrayValue paraVarBinary = new ();
    ArrayValue paraBlob = new ();
    int rowId = 8;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array,
         string_array, smallint_array, numeric_array, real_array, char_array, varchar_array, nvarchar_array, date_array, time_array, datetime_array, timestamp_array, binary_array, varbinary_array, blob_array) VALUES(${rowId}, ${paraInt}, ${paraLong}, ${paraFloat}, ${paraDouble}, ${paraDecimal},
         ${paraBool}, ${paraString}, ${paraSmallint}, ${paraNumeric}, ${paraReal}, ${paraChar}, ${paraVarchar}, ${paraNVarchar}, ${paraDate}, ${paraTime}, ${paraDatetime}, ${paraTimestamp}, ${paraBinary}, ${paraVarBinary}, ${paraBlob})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable5() returns error? {
    SmallIntValue smallintValue1 = new ();
    SmallIntValue smallintValue2 = new ();
    SmallIntValue[] datasmallint = [smallintValue1, smallintValue2];
    IntegerValue integerValue1 = new ();
    IntegerValue integerValue2 = new ();
    IntegerValue[] dataint = [integerValue1, integerValue2];
    BigIntValue bigIntValue1 = new ();
    BigIntValue bigIntValue2 = new ();
    BigIntValue[] datalong = [bigIntValue1, bigIntValue2];
    FloatValue floatValue1 = new ();
    FloatValue floatValue2 = new ();
    FloatValue[] datafloat = [floatValue1, floatValue2];
    DoubleValue doubleValue1 = new ();
    DoubleValue doubleValue2 = new ();
    DoubleValue[] datadouble = [doubleValue1, doubleValue2];
    RealValue realValue1 = new ();
    RealValue realValue2 = new ();
    RealValue[] dataReal = [realValue1, realValue2];
    DecimalValue decimalValue1 = new ();
    DecimalValue decimalValue2 = new ();
    DecimalValue[] datadecimal = [decimalValue1, decimalValue2];
    NumericValue numericValue1 = new ();
    NumericValue numericValue2 = new ();
    NumericValue[] dataNumeric = [numericValue1, numericValue2];
    CharValue charValue1 = new ();
    CharValue charValue2 = new ();
    CharValue[] dataChar = [charValue1, charValue2];
    NVarcharValue nvarcharValue1 = new ();
    NVarcharValue nvarcharValue2 = new ();
    NVarcharValue[] dataNVarchar = [nvarcharValue1, nvarcharValue2];
    BooleanValue trueValue = new ();
    BooleanValue falseValue = new ();
    BooleanValue[] databoolean = [trueValue, falseValue, trueValue];
    DateValue date1 = new ();
    DateValue date2 = new ();
    DateValue[] dataDate = [date1, date2];
    TimeValue time1 = new ();
    TimeValue time2 = new ();
    TimeValue[] dataTime = [time1, time2];
    DateTimeValue datetime1 = new ();
    DateTimeValue datetime2 = new ();
    DateTimeValue[] dataDatetime = [datetime1, datetime2];
    TimestampValue timestamp1 = new ();
    TimestampValue timestamp2 = new ();
    TimestampValue[] dataTimestamp = [timestamp1, timestamp2];
    BinaryValue binary1 = new ();
    BinaryValue binary2 = new ();
    BinaryValue[] dataBinary = [binary1, binary2];
    VarBinaryValue varBinary1 = new ();
    VarBinaryValue varBinary2 = new ();
    VarBinaryValue[] dataVarBinary = [varBinary1, varBinary2];

    ArrayValue paraSmallint = new (datasmallint);
    ArrayValue paraInt = new (dataint);
    ArrayValue paraLong = new (datalong);
    ArrayValue paraFloat = new (datafloat);
    ArrayValue paraReal = new (dataReal);
    ArrayValue paraDecimal = new (datadecimal);
    ArrayValue paraNumeric = new (dataNumeric);
    ArrayValue paraDouble = new (datadouble);
    ArrayValue paraChar = new (dataChar);
    VarcharArrayValue paraVarchar = new ([(), ()]);
    ArrayValue paraNVarchar = new (dataNVarchar);
    ArrayValue paraBool = new (databoolean);
    ArrayValue paraDate = new (dataDate);
    ArrayValue paraTime = new (dataTime);
    ArrayValue paraDatetime = new (dataDatetime);
    ArrayValue paraTimestamp = new (dataTimestamp);
    ArrayValue paraBinary = new (dataBinary);
    ArrayValue paraVarBinary = new (dataVarBinary);
    int rowId = 9;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array,
         smallint_array, numeric_array, real_array, char_array, varchar_array, nvarchar_array, date_array, time_array, datetime_array, timestamp_array, binary_array, varbinary_array) VALUES(${rowId}, ${paraInt}, ${paraLong}, ${paraFloat}, ${paraDouble}, ${paraDecimal},
         ${paraBool}, ${paraSmallint}, ${paraNumeric}, ${paraReal}, ${paraChar}, ${paraVarchar}, ${paraNVarchar}, ${paraDate}, ${paraTime}, ${paraDatetime}, ${paraTimestamp}, ${paraBinary}, ${paraVarBinary})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable6() returns error? {
    decimal decimal1 = 19.21;
    decimal decimal2 = 492.98;
    FloatValue floatValue1 = new (1);
    FloatValue floatValue2 = new (4);
    FloatValue[] datafloat = [floatValue1, floatValue2];
    DoubleValue doubleValue1 = new (decimal1);
    DoubleValue doubleValue2 = new (decimal2);
    DoubleValue[] datadouble = [doubleValue1, doubleValue2];
    RealValue realValue1 = new (decimal1);
    RealValue realValue2 = new (decimal2);
    RealValue[] dataReal = [realValue1, realValue2];
    DecimalValue decimalValue1 = new (decimal1);
    DecimalValue decimalValue2 = new (decimal2);
    DecimalValue[] datadecimal = [decimalValue1, decimalValue2];
    NumericValue numericValue1 = new (decimal1);
    NumericValue numericValue2 = new (decimal2);
    NumericValue[] dataNumeric = [numericValue1, numericValue2];
    DateValue date1 = new ("2021-12-18");
    DateValue date2 = new ("2021-12-19");
    DateValue[] dataDate = [date1, date2];
    TimeValue time1 = new ("20:08:59");
    TimeValue time2 = new ("21:18:59");
    TimeValue[] dataTime = [time1, time2];
    DateTimeValue datetime1 = new ("2008-08-08 20:08:08");
    DateTimeValue datetime2 = new ("2009-09-09 23:09:09");
    DateTimeValue[] dataDatetime = [datetime1, datetime2];
    TimestampValue timestamp1 = new ("2008-08-08 20:08:08");
    TimestampValue timestamp2 = new ("2008-08-08 20:08:09");
    TimestampValue[] dataTimestamp = [timestamp1, timestamp2];
    io:ReadableByteChannel byteChannel1 = check getByteColumnChannel();
    io:ReadableByteChannel byteChannel2 = check getByteColumnChannel();
    BinaryValue binary1 = new (byteChannel1);
    BinaryValue binary2 = new (byteChannel2);
    io:ReadableByteChannel varbinaryChannel1 = check getBlobColumnChannel();
    io:ReadableByteChannel varbinaryChannel2 = check getBlobColumnChannel();
    BinaryValue[] dataBinary = [binary1, binary2];
    VarBinaryValue varBinary1 = new (varbinaryChannel1);
    VarBinaryValue varBinary2 = new (varbinaryChannel2);
    VarBinaryValue[] dataVarBinary = [varBinary1, varBinary2];

    ArrayValue paraFloat = new (datafloat);
    ArrayValue paraReal = new (dataReal);
    ArrayValue paraDecimal = new (datadecimal);
    ArrayValue paraNumeric = new (dataNumeric);
    ArrayValue paraDouble = new (datadouble);
    ArrayValue paraDate = new (dataDate);
    ArrayValue paraTime = new (dataTime);
    ArrayValue paraDatetime = new (dataDatetime);
    ArrayValue paraTimestamp = new (dataTimestamp);
    ArrayValue paraBinary = new (dataBinary);
    ArrayValue paraVarBinary = new (dataVarBinary);
    int rowId = 10;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id,float_array, double_array, decimal_array,
         numeric_array, real_array, date_array, time_array, datetime_array, timestamp_array, binary_array, varbinary_array) VALUES(${rowId}, ${paraFloat}, ${paraDouble}, ${paraDecimal},
         ${paraNumeric}, ${paraReal}, ${paraDate}, ${paraTime}, ${paraDatetime}, ${paraTimestamp}, ${paraBinary}, ${paraVarBinary})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable7() returns error? {
    int int1 = 19;
    int int2 = 492;
    DoubleValue doubleValue1 = new (int1);
    DoubleValue doubleValue2 = new (int2);
    DoubleValue[] datadouble = [doubleValue1, doubleValue2];
    RealValue realValue1 = new (int1);
    RealValue realValue2 = new (int2);
    RealValue[] dataReal = [realValue1, realValue2];
    DecimalValue decimalValue1 = new (int1);
    DecimalValue decimalValue2 = new (int2);
    DecimalValue[] datadecimal = [decimalValue1, decimalValue2];
    NumericValue numericValue1 = new (int1);
    NumericValue numericValue2 = new (int2);
    NumericValue[] dataNumeric = [numericValue1, numericValue2];

    ArrayValue paraReal = new (dataReal);
    ArrayValue paraDecimal = new (datadecimal);
    ArrayValue paraNumeric = new (dataNumeric);
    ArrayValue paraDouble = new (datadouble);
    int rowId = 11;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id, double_array, decimal_array,
         numeric_array, real_array) VALUES(${rowId}, ${paraDouble}, ${paraDecimal},
         ${paraNumeric}, ${paraReal})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable8() returns error? {
    DateValue date1 = new ("2021-12-18+8.00");
    DateValue date2 = new ("2021-12-19+8.00");
    DateValue[] dataDate = [date1, date2];
    TimeValue time1 = new ("20:08:59+8.00");
    TimeValue time2 = new ("21:18:59+8.00");
    TimeValue[] dataTime = [time1, time2];
    DateTimeValue datetime1 = new ("2008-08-08 20:08:08+8.00");
    DateTimeValue datetime2 = new ("2009-09-09 23:09:09+8.00");
    DateTimeValue[] dataDatetime = [datetime1, datetime2];
    TimestampValue timestamp1 = new ("2008-08-08 20:08:08+8.00");
    TimestampValue timestamp2 = new ("2008-08-08 20:08:09+8.00");
    TimestampValue[] dataTimestamp = [timestamp1, timestamp2];

    ArrayValue paraDate = new (dataDate);
    ArrayValue paraTime = new (dataTime);
    ArrayValue paraDatetime = new (dataDatetime);
    ArrayValue paraTimestamp = new (dataTimestamp);
    int rowId = 12;

    ParameterizedQuery sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id, date_array) VALUES(${rowId}, ${paraDate})`;
    ExecutionResult | error result = executeQueryMockClient(sqlQuery);
    test:assertTrue(result is error, "Error Expected for date array");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported String Value"));

    sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id, time_array) VALUES(${rowId}, ${paraTime})`;
    result = executeQueryMockClient(sqlQuery);
    test:assertTrue(result is error, "Error Expected for time array");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported String Value"));

    sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id, datetime_array) VALUES(${rowId}, ${paraDatetime})`;
    result = executeQueryMockClient(sqlQuery);
    test:assertTrue(result is error, "Error Expected for datetime array");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported String Value"));

    sqlQuery =
        `INSERT INTO ArrayTypes2 (row_id, timestamp_array) VALUES(${rowId}, ${paraTimestamp})`;
    result = executeQueryMockClient(sqlQuery);
    test:assertTrue(result is error, "Error Expected for timestamp array");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported String Value"));
}

function executeQueryMockClient(ParameterizedQuery sqlQuery)
returns ExecutionResult | error {
    MockClient dbClient = check new (url = executeParamsDb, user = user, password = password);
    ExecutionResult result = check dbClient->execute(sqlQuery);
    check dbClient.close();
    return result;
}

isolated function validateResult(ExecutionResult result, int rowCount, int? lastId = ()) {
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
