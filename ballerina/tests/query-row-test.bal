// Copyright (c) 2021 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/test;
import ballerina/io;
import ballerina/time;

string queryRowDb = urlPrefix + "9010/queryrow";

@test:BeforeGroups {
	value: ["query-rowx"]
}
function initQueryRowContainer() returns error? {
	check initializeDockerContainer("sql-query-row", "queryrow", "9010", "query", "query-row-test-data.sql");
}

@test:AfterGroups {
	value: ["query-rowx"]
}
function cleanQueryRowContainer() returns error? {
	check cleanDockerContainer("sql-query-row");
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordSingleIntParam() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDoubleIntParam() returns error? {
    int rowId = 1;
    int intType = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId} AND int_type =  ${intType}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordIntAndLongParam() returns error? {
    int rowId = 1;
    int longType = 9223372036854774807;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId} AND long_type = ${longType}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordStringParam() returns error? {
    string stringType = "Hello";
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${stringType}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordIntAndStringParam() returns error? {
    string stringType = "Hello";
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${stringType} AND row_id = ${rowId}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDoubleParam() returns error? {
    float doubleType = 2139095039.0;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE double_type = ${doubleType}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordFloatParam() returns error? {
    float floatType = 123.34;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE float_type = ${floatType}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDoubleAndFloatParam()  returns error? {
    float floatType = 123.34;
    float doubleType = 2139095039.0;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE float_type = ${floatType}
                                                                    and double_type = ${doubleType}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDecimalParam()  returns error? {
    decimal decimalValue = 23.45;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE decimal_type = ${decimalValue}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDecimalAnFloatParam()  returns error? {
    decimal decimalValue = 23.45;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE decimal_type = ${decimalValue}
                                                                    and double_type = 2139095039.0`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordVarcharStringParam()  returns error? {
    VarcharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeCharStringParam()  returns error? {
    CharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeNCharStringParam() returns error? {
    NCharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeNVarCharStringParam() returns error? {
    NCharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeVarCharIntegerParam()  returns error? {
    int intVal = 1;
    NCharValue typeVal = new (intVal.toString());
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;

    record {}? returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertNotEquals(returnData, ());
    if returnData is () {
        test:assertFail("Query returns ()");
    } else {
        decimal decimalVal = 25.45;
        test:assertEquals(returnData["INT_TYPE"], 1);
        test:assertEquals(returnData["LONG_TYPE"], 9372036854774807);
        test:assertEquals(returnData["DOUBLE_TYPE"], <float> 29095039);
        test:assertEquals(returnData["BOOLEAN_TYPE"], false);
        test:assertEquals(returnData["DECIMAL_TYPE"], decimalVal);
        test:assertEquals(returnData["STRING_TYPE"], "1");
        test:assertTrue(returnData["FLOAT_TYPE"] is float);
        test:assertEquals(returnData["ROW_ID"], 3);
    }
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeBooleanBooleanParam() returns error? {
    BooleanValue typeVal = new (true);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeBitIntParam() returns error? {
    BitValue typeVal = new (1);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeBitInvalidIntParam() returns error? {
    BitValue typeVal = new (12);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    record{}|error returnVal = queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertTrue(returnVal is error);
    error dbError = <error> returnVal;
    test:assertTrue(dbError.message().endsWith("Only 1 or 0 can be passed for BitValue SQL Type, but found :12"));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeBitStringParam() returns error? {
    BitValue typeVal = new (true);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordIntTypeInvalidParam() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT int_type from DataTable WHERE row_id = ${rowId}`;
    MockClient dbClient = check new (url = queryRowDb, user = user, password = password);
    DataTableRecord|error queryResult = dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertTrue(queryResult is error);
    if queryResult is ApplicationError {
        test:assertTrue(queryResult.message().endsWith("The field 'int_type' of type float cannot be mapped to the column " +
        "'INT_TYPE' of SQL type 'INTEGER'"));
    } else {
        test:assertFail("ApplicationError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeIntIntParam() returns error? {
    IntegerValue typeVal = new (2147483647);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE int_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeTinyIntIntParam() returns error? {
    SmallIntValue typeVal = new (127);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE tinyint_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeSmallIntIntParam() returns error? {
    SmallIntValue typeVal = new (32767);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE smallint_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeBigIntIntParam() returns error? {
    BigIntValue typeVal = new (9223372036854774807);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE bigint_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeDoubleDoubleParam() returns error? {
    DoubleValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeDoubleIntParam() returns error? {
    DoubleValue typeVal = new (1234);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    record{} returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertEquals(returnData.length(), 10);
    test:assertEquals(returnData["ID"], 2);
    test:assertEquals(returnData["REAL_TYPE"], 1234.0);
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeDoubleDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    DoubleValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeFloatDoubleParam() returns error? {
    DoubleValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeRealDoubleParam() returns error? {
    RealValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE real_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeNumericDoubleParam() returns error? {
    NumericValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeNumericIntParam() returns error? {
    NumericValue typeVal = new (1234);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    record{} returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertEquals(returnData.length(), 10);
    test:assertEquals(returnData["ID"], 2);
    test:assertEquals(returnData["REAL_TYPE"], 1234.0);
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeNumericDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    NumericValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeDecimalDoubleParam() returns error? {
    DecimalValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE decimal_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeDecimalDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    DecimalValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE decimal_type = ${typeVal}`;
    validateNumericTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordByteArrayParam() returns error? {
    record {} value = check queryRecordMockClient(queryRowDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BINARY_TYPE");
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${binaryData}`;
    validateComplexTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeBinaryByteParam() returns error? {
    record {} value = check queryRecordMockClient(queryRowDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BINARY_TYPE");
    BinaryValue typeVal = new (binaryData);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${typeVal}`;
    validateComplexTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeBinaryReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();
    BinaryValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${typeVal}`;
    validateComplexTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeVarBinaryReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();
    VarBinaryValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE var_binary_type = ${typeVal}`;
    validateComplexTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeTinyBlobByteParam() returns error? {
    record {}|error? value = check queryMockClient(queryRowDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BLOB_TYPE");
    BinaryValue typeVal = new (binaryData);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE blob_type = ${typeVal}`;
    validateComplexTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeClobStringParam() returns error? {
    ClobValue typeVal = new ("very long text");
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeClobReadableCharChannelParam() returns error? {
    io:ReadableCharacterChannel clobChannel = check getClobColumnChannel();
    ClobValue typeVal = new (clobChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTypeNClobReadableCharChannelParam() returns error? {
    io:ReadableCharacterChannel clobChannel = check getClobColumnChannel();
    NClobValue typeVal = new (clobChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDateStringParam() returns error? {
    DateValue typeVal = new ("2017-02-03");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDateString2Param() returns error? {
    DateValue typeVal = new ("2017-2-3");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDateStringInvalidParam() {
    DateValue typeVal = new ("2017/2/3");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    record{}|error result = trap queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertTrue(result is error);

    if result is ApplicationError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from " +
                "DateTimeTypes WHERE date_type =  ? . java.lang.IllegalArgumentException"));
    } else {
        test:assertFail("ApplicationError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDateTimeRecordParam() returns error? {
    time:Date date = {year: 2017, month:2, day: 3};
    DateValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimestampWithTimeZoneRecordParam() returns error? {
    time:Civil dateTime = {utcOffset: {hours: +8, minutes: 0}, year:2008, month:8, day:8, hour: 20, minute: 8,
                            second:8};
    DateTimeValue typeVal = new (dateTime);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type2 = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimeStringParam() returns error? {
    TimeValue typeVal = new ("11:35:45");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimeStringInvalidParam() {
    TimeValue typeVal = new ("11-35-45");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    record{}|error? result = trap queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertTrue(result is error);

    if result is DatabaseError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from DateTimeTypes " +
        "WHERE time_type =  ? . data exception: invalid datetime format."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimeTimeRecordParam() returns error? {
    time:TimeOfDay date = {hour: 11, minute: 35, second:45};
    TimeValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimeTimeRecordWithTimeZoneParam() returns error? {
    time:TimeOfDay time = {utcOffset: {hours: -8, minutes: 0}, hour: 4, minute: 8, second: 8};
    TimeValue typeVal = new (time);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type2 = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimestampStringParam() returns error? {
    TimestampValue typeVal = new ("2017-02-03 11:53:00");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimestampStringInvalidParam() {
    TimestampValue typeVal = new ("2017/02/03 11:53:00");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    record{}|error result = queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertTrue(result is error);

    if result is DatabaseError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from DateTimeTypes " +
        "WHERE timestamp_type =  ? . data exception: invalid datetime format."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimestampTimeRecordParam() returns error? {
    time:Utc date = check time:utcFromString("2017-02-03T11:53:00.00Z");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimestampTimeRecordWithTimeZoneParam() returns error? {
    time:Utc date = check time:utcFromString("2017-02-03T11:53:00.00Z");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordDateTimeTimeRecordWithTimeZoneParam() returns error? {
    time:Civil date = {year: 2017, month:2, day: 3, hour: 11, minute: 53, second:0};
    DateTimeValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE datetime_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordTimestampTimeRecordWithTimeZone2Param() returns error? {
    time:Utc date = check time:utcFromString("2008-08-08T20:08:08+08:00");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type2 = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordArrayBasicParams() returns error? {
    int[] paraInt = [1, 2, 3];
    int[] paraLong = [10000, 20000, 30000];
    float[] paraFloat = [245.23, 5559.49, 8796.123];
    float[] paraDouble = [245.23, 5559.49, 8796.123];
    decimal[] paraDecimal = [245, 5559, 8796];
    string[] paraString = ["Hello", "Ballerina"];
    boolean[] paraBool = [true, false, true];

    ParameterizedQuery sqlQuery =
    `SELECT * from ArrayTypes WHERE int_array = ${paraInt}
                                AND long_array = ${paraLong}
                                AND float_array = ${paraFloat}
                                AND double_array = ${paraDouble}
                                AND decimal_array = ${paraDecimal}
                                AND string_array = ${paraString}
                                AND boolean_array = ${paraBool}`;
    record{} returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    if returnData is record{} {
        test:assertEquals(returnData["INT_ARRAY"], [1, 2, 3]);
        test:assertEquals(returnData["LONG_ARRAY"], [10000, 20000, 30000]);
        test:assertEquals(returnData["BOOLEAN_ARRAY"], [true, false, true]);
        test:assertEquals(returnData["STRING_ARRAY"], ["Hello", "Ballerina"]);
        test:assertNotEquals(returnData["FLOAT_ARRAY"], ());
        test:assertNotEquals(returnData["DECIMAL_ARRAY"], ());
        test:assertNotEquals(returnData["DOUBLE_ARRAY"], ());
    } else {
        test:assertFail("Empty row returned.");
    }
}

@test:Config {
    groups: ["query", "query-rowx"]
}
function queryRecordArrayBasicNullParams() returns error? {
    ParameterizedQuery sqlQuery =
        `SELECT * from ArrayTypes WHERE int_array is null AND long_array is null AND float_array
         is null AND double_array is null AND decimal_array is null AND string_array is null
         AND boolean_array is null`;

    record{} returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    if returnData is record{} {
        test:assertEquals(returnData["INT_ARRAY"], ());
        test:assertEquals(returnData["LONG_ARRAY"], ());
        test:assertEquals(returnData["FLOAT_ARRAY"], ());
        test:assertEquals(returnData["DECIMAL_ARRAY"], ());
        test:assertEquals(returnData["DOUBLE_ARRAY"], ());
        test:assertEquals(returnData["BOOLEAN_ARRAY"], ());
        test:assertEquals(returnData["STRING_ARRAY"], ());
        test:assertEquals(returnData["BLOB_ARRAY"], ());
    } else {
        test:assertFail("Empty row returned.");
    }
}



















