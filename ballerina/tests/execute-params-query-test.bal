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
    DateTimeValue dateTimeVal = new ("2017-02-03 11:53:00");
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
    DateTimeValue dateTimeVal = new ();
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

type DateTimeResultRecord record {
    time:Date date_type;
    time:TimeOfDay time_type;
    time:Civil datetime_type;
    time:Civil timestamp_tz_type;
};

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoDateTimeTable5() returns error? {
    int rowId = 6;
    time:Date dateRecord = {year: 2020, month: 9, day: 8};
    time:TimeOfDay timeRecord = {hour: 7, minute: 10, second: 59};
    time:Civil civilRecord = {
        year: 2017,
        month: 1,
        day: 25,
        hour: 16,
        minute: 33,
        second: 55
    };
    time:Civil timestampWithTimezoneRecord = {
        utcOffset: {hours: -8, minutes: 0},
        timeAbbrev: "-08:00",
        year: 2017,
        month: 1,
        day: 25,
        hour: 16,
        minute: 33,
        second: 55
    };

    ParameterizedQuery sqlQuery = `
        INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_tz_type)
        VALUES(${rowId}, ${dateRecord}, ${timeRecord}, ${civilRecord}, ${timestampWithTimezoneRecord})
    `;
    validateResult(check executeQueryMockClient(sqlQuery), 1);

    MockClient dbClient = check new (url = executeParamsDb, user = user, password = password);
    stream<record {}, error?> queryResult = dbClient->query(`
        SELECT date_type, time_type, datetime_type, timestamp_tz_type
        FROM DateTimeTypes WHERE row_id = ${rowId}
    `, DateTimeResultRecord);
    record {|record {} value;|}? data = check queryResult.next();
    record {}? value = data?.value;
    check dbClient.close();

    DateTimeResultRecord expected = {
        date_type: dateRecord,
        time_type: timeRecord,
        datetime_type: civilRecord,
        timestamp_tz_type: timestampWithTimezoneRecord
    };

    test:assertEquals(value, expected, "Inserted data did not match retrieved data.");
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoDateTimeTable6() returns error? {
    int rowId = 7;
    time:Civil timeCivil = {
        utcOffset: {hours: -8, minutes: 0},
        timeAbbrev: "-08:00",
        year: 2017,
        month: 1,
        day: 25,
        hour: 16,
        minute: 33,
        second: 55
    };
    time:Utc timeUtc = check time:utcFromCivil(timeCivil);

    ParameterizedQuery sqlQuery =
            `INSERT INTO DateTimeTypes (row_id, timestamp_tz_type) VALUES(${rowId}, ${timeUtc})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);

    MockClient dbClient = check new (url = executeParamsDb, user = user, password = password);
    time:Utc retrievedTimeUtc = check dbClient->queryRow(`
        SELECT timestamp_tz_type FROM DateTimeTypes WHERE row_id = ${rowId}
    `);
    check dbClient.close();

    test:assertEquals(retrievedTimeUtc, timeUtc, "Inserted data did not match retrieved data.");
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable() returns error? {
    int[] paraInt = [1, 2, 3];
    int[] paraLong = [100000000, 200000000, 300000000];
    float[] paraFloat = [245.23, 5559.49, 8796.123];
    float[] paraDouble = [245.23, 5559.49, 8796.123];
    decimal[] paraDecimal = [245, 5559, 8796];
    string[] paraString = ["Hello", "Ballerina"];
    boolean[] paraBool = [true, false, true];

    record {}? value = check queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[][] paraBlob = [<byte[]>getUntaintedData(value, "BLOB_TYPE")];

    int rowId = 5;

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
    SmallIntArrayValue paraSmallint = new ([1211, 478]);
    IntegerArrayValue paraInt = new ([121, 498]);
    BigIntArrayValue paraLong = new ([121, 498]);
    float[] paraFloat = [19.21, 492.98];
    DoubleArrayValue paraDouble = new ([float1, float2]);
    RealArrayValue paraReal = new ([float1, float2]);
    DecimalArrayValue paraDecimal = new ([<decimal>12.245, <decimal>13.245]);
    NumericArrayValue paraNumeric = new ([float1, float2]);
    CharArrayValue paraChar = new (["Char value", "Character"]);
    VarcharArrayValue paraVarchar = new (["Varchar value", "Varying Char"]);
    NVarcharArrayValue paraNVarchar = new (["NVarchar value", "Varying NChar"]);
    string[] paraString = ["Hello", "Ballerina"];
    BooleanArrayValue paraBool = new ([true, false]);
    BitArrayValue paraBit = new ([true, false]);
    DateArrayValue paraDate = new (["2021-12-18", "2021-12-19"]);
    time:TimeOfDay time = {hour: 20, minute: 8, second: 12};
    TimeArrayValue paraTime = new ([time, time]);
    time:Civil datetime = {year: 2021, month: 12, day: 18, hour: 20, minute: 8, second: 12};
    DateTimeArrayValue paraDatetime = new ([datetime, datetime]);
    time:Utc timestampUtc = [12345600, 12];
    TimestampArrayValue paraTimestamp = new ([timestampUtc, timestampUtc]);
    byte[] byteArray1 = [1, 2, 3];
    byte[] byteArray2 = [4, 5, 6];
    BinaryArrayValue paraBinary = new ([byteArray1, byteArray2]);
    VarBinaryArrayValue paraVarBinary = new ([byteArray1, byteArray2]);
    io:ReadableByteChannel byteChannel = check getBlobColumnChannel();
    record {}? value = check queryMockClient(executeParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[][] paraBlob = [<byte[]>getUntaintedData(value, "BLOB_TYPE")];
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
    SmallIntArrayValue paraSmallint = new ([]);
    IntegerArrayValue paraInt = new ([]);
    BigIntArrayValue paraLong = new ([]);
    FloatArrayValue paraFloat = new (<int?[]>[]);
    RealArrayValue paraReal = new (<int?[]>[]);
    DecimalArrayValue paraDecimal = new (<int?[]>[]);
    NumericArrayValue paraNumeric = new (<int?[]>[]);
    DoubleArrayValue paraDouble = new (<int?[]>[]);
    CharArrayValue paraChar = new ([]);
    VarcharArrayValue paraVarchar = new ([]);
    NVarcharArrayValue paraNVarchar = new ([]);
    string?[] paraString = [];
    BooleanArrayValue paraBool = new ([]);
    DateArrayValue paraDate = new (<string?[]>[]);
    TimeArrayValue paraTime = new (<string?[]>[]);
    DateTimeArrayValue paraDatetime = new (<string?[]>[]);
    TimestampArrayValue paraTimestamp = new (<string?[]>[]);
    BinaryArrayValue paraBinary = new (<byte[]?[]>[]);
    VarBinaryArrayValue paraVarBinary = new (<byte[]?[]>[]);
    byte[]?[] paraBlob = [];
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
    SmallIntArrayValue paraSmallint = new ([null, null]);
    IntegerArrayValue paraInt = new ([null, null]);
    BigIntArrayValue paraLong = new ([null, null]);
    FloatArrayValue paraFloat = new (<int?[]>[null, null]);
    RealArrayValue paraReal = new (<int?[]>[null, null]);
    DecimalArrayValue paraDecimal = new (<int?[]>[null, null]);
    NumericArrayValue paraNumeric = new (<int?[]>[null, null]);
    DoubleArrayValue paraDouble = new (<int?[]>[null, null]);
    CharArrayValue paraChar = new ([null, null]);
    VarcharArrayValue paraVarchar = new ([(), ()]);
    NVarcharArrayValue paraNVarchar = new ([null, null]);
    BooleanArrayValue paraBool = new ([null, null]);
    DateArrayValue paraDate = new (<string?[]>[null, null]);
    TimeArrayValue paraTime = new (<string?[]>[null, null]);
    DateTimeArrayValue paraDatetime = new (<string?[]>[null, null]);
    TimestampArrayValue paraTimestamp = new (<string?[]>[null, null]);
    BinaryArrayValue paraBinary = new (<byte[]?[]>[null, null]);
    VarBinaryArrayValue paraVarBinary = new (<byte[]?[]>[null, null]);
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
    int int1 = 1;
    int int2 = 4;
    FloatArrayValue paraFloat = new ([int1, int2]);
    RealArrayValue paraReal = new ([decimal1, decimal2]);
    DecimalArrayValue paraDecimal = new ([decimal1, decimal2]);
    NumericArrayValue paraNumeric = new ([decimal1, decimal2]);
    DoubleArrayValue paraDouble = new ([decimal1, decimal2]);
    DateArrayValue paraDate = new (["2021-12-18", "2021-12-19"]);
    TimeArrayValue paraTime = new (["20:08:59", "21:18:59"]);
    DateTimeArrayValue paraDatetime = new (["2008-08-08 20:08:08", "2009-09-09 23:09:09"]);
    TimestampArrayValue paraTimestamp = new (["2008-08-08 20:08:08", "2008-08-08 20:08:09"]);
    io:ReadableByteChannel byteChannel1 = check getByteColumnChannel();
    io:ReadableByteChannel byteChannel2 = check getByteColumnChannel();
    BinaryArrayValue paraBinary = new ([byteChannel1, byteChannel2]);
    io:ReadableByteChannel varbinaryChannel1 = check getBlobColumnChannel();
    io:ReadableByteChannel varbinaryChannel2 = check getBlobColumnChannel();
    VarBinaryArrayValue paraVarBinary = new ([varbinaryChannel1, varbinaryChannel2]);
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
    DoubleArrayValue paraReal = new ([int1, int2]);
    RealArrayValue paraDecimal = new ([int1, int2]);
    DecimalArrayValue paraNumeric = new ([int1, int2]);
    NumericArrayValue paraDouble = new ([int1, int2]);
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
    DateArrayValue paraDate = new (["2021-12-18+8.00", "2021-12-19+8.00"]);
    TimeArrayValue paraTime = new (["20:08:59+8.00", "21:18:59+8.00"]);
    DateTimeArrayValue paraDatetime = new (["2008-08-08 20:08:08+8.00", "2009-09-09 23:09:09+8.00"]);
    TimestampArrayValue paraTimestamp = new (["2008-08-08 20:08:08+8.00", "2008-08-08 20:08:09+8.00"]);
    int rowId = 12;

    ParameterizedQuery sqlQuery = 
        `INSERT INTO ArrayTypes2 (row_id, date_array) VALUES(${rowId}, ${paraDate})`;
    ExecutionResult|error result = executeQueryMockClient(sqlQuery);
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

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable9() returns error? {
    time:TimeOfDay timeRecord = {hour: 14, minute: 15, second: 23};
    TimeArrayValue paraTime = new ([timeRecord]);

    time:Date dateRecord = {year: 2017, month: 5, day: 23};
    DateArrayValue paraDate = new ([dateRecord]);

    time:Utc timestampRecord = time:utcNow();
    TimestampArrayValue paraTimestamp = new ([timestampRecord]);

    int rowId = 13;

    ParameterizedQuery sqlQuery = 
        `INSERT INTO ArrayTypes2 (row_id, time_array, date_array, timestamp_array) VALUES(${rowId},
                ${paraTime}, ${paraDate}, ${paraTimestamp})`;
    validateResult(check executeQueryMockClient(sqlQuery), 1);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable10() returns error? {
    time:TimeOfDay timeWithTzRecord = {utcOffset: {hours: 6, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+06:30"};
    TimeArrayValue paraTimeWithTZ = new ([timeWithTzRecord]);

    int rowId = 14;
    ParameterizedQuery sqlQuery = 
        `INSERT INTO ArrayTypes2 (row_id, time_tz_array) VALUES(${rowId},
                ${paraTimeWithTZ})`;
    ExecutionResult|error result = executeQueryMockClient(sqlQuery);
    test:assertTrue(result is error, "Error Expected for timestamp array");
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoArrayTable11() returns error? {
    time:Civil timestampWithTzRecord = {
        utcOffset: {hours: -8, minutes: 0},
        timeAbbrev: "-08:00",
        year: 2017,
        month: 1,
        day: 25,
        hour: 16,
        minute: 33,
        second: 55
    };
    DateTimeArrayValue paraDatetimeWithTZ = new ([timestampWithTzRecord]);
    int rowId = 14;
    ParameterizedQuery sqlQuery = 
        `INSERT INTO ArrayTypes2 (row_id, timestamp_tz_array) VALUES(${rowId},
                ${paraDatetimeWithTZ})`;
    ExecutionResult|error result = executeQueryMockClient(sqlQuery);
    test:assertTrue(result is error, "Error Expected for timestamp array");
}

function executeQueryMockClient(ParameterizedQuery sqlQuery) 
returns ExecutionResult|error {
    MockClient dbClient = check new (url = executeParamsDb, user = user, password = password);
    ExecutionResult result = check dbClient->execute(sqlQuery);
    check dbClient.close();
    return result;
}

isolated function validateResult(ExecutionResult result, int rowCount, int? lastId = ()) {
    test:assertExactEquals(result.affectedRowCount, rowCount, "Affected row count is different.");

    if lastId is () {
        test:assertEquals(result.lastInsertId, (), "Last Insert Id is not nil.");
    } else {
        int|string? lastInsertIdVal = result.lastInsertId;
        if lastInsertIdVal is int {
            test:assertTrue(lastInsertIdVal > 1, "Last Insert Id is nil.");
        } else {
            test:assertFail("The last insert id should be an integer found type '" + lastInsertIdVal.toString());
        }
    }
}
