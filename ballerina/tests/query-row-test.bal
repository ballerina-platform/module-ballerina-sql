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

import ballerina/io;
import ballerina/test;
import ballerina/time;

string queryRowDb = urlPrefix + "9011/queryrow";

@test:BeforeGroups {
	value: ["query-row"]
}
function initQueryRowContainer() returns error? {
	check initializeDockerContainer("sql-query-row", "queryrow", "9011", "query", "query-row-test-data.sql");
}

@test:AfterGroups {
	value: ["query-row"]
}
function cleanQueryRowContainer() returns error? {
	check cleanDockerContainer("sql-query-row");
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordSingleIntParam() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDoubleIntParam() returns error? {
    int rowId = 1;
    int intType = 1;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE row_id = ${rowId} AND int_type =  ${intType}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordIntAndLongParam() returns error? {
    int rowId = 1;
    int longType = 9223372036854774807;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE row_id = ${rowId} AND long_type = ${longType}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordStringParam() returns error? {
    string stringType = "Hello";
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE string_type = ${stringType}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordIntAndStringParam() returns error? {
    string stringType = "Hello";
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE string_type = ${stringType} AND row_id = ${rowId}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDoubleParam() returns error? {
    float doubleType = 2139095039.0;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE double_type = ${doubleType}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordFloatParam() returns error? {
    float floatType = 123.34;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE float_type = ${floatType}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDoubleAndFloatParam()  returns error? {
    float floatType = 123.34;
    float doubleType = 2139095039.0;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE float_type = ${floatType}
                                                                    and double_type = ${doubleType}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDecimalParam()  returns error? {
    decimal decimalValue = 23.45;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE decimal_type = ${decimalValue}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDecimalAnFloatParam()  returns error? {
    decimal decimalValue = 23.45;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE decimal_type = ${decimalValue}
                                                                    and double_type = 2139095039.0`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordVarcharStringParam()  returns error? {
    VarcharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE string_type = ${typeVal}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeCharStringParam()  returns error? {
    CharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE string_type = ${typeVal}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeNCharStringParam() returns error? {
    NCharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE string_type = ${typeVal}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeNVarCharStringParam() returns error? {
    NCharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE string_type = ${typeVal}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeVarCharIntegerParam()  returns error? {
    int intVal = 1;
    NCharValue typeVal = new (intVal.toString());
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE string_type = ${typeVal}`;

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
    groups: ["query", "query-row"]
}
function queryRecordTypeBooleanBooleanParam() returns error? {
    BooleanValue typeVal = new (true);
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeBitIntParam() returns error? {
    BitValue typeVal = new (1);
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeBitInvalidIntParam() returns error? {
    BitValue typeVal = new (12);
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE boolean_type = ${typeVal}`;
    record{}|error returnVal = queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertTrue(returnVal is error);
    error dbError = <error> returnVal;
    test:assertTrue(dbError.message().endsWith("Only 1 or 0 can be passed for BitValue SQL Type, but found :12"));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeBitStringParam() returns error? {
    BitValue typeVal = new (true);
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordIntTypeInvalidParam() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT int_type FROM DataTable WHERE row_id = ${rowId}`;
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
    groups: ["query", "query-row"]
}
function queryRecordTypeIntIntParam() returns error? {
    IntegerValue typeVal = new (2147483647);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE int_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeTinyIntIntParam() returns error? {
    SmallIntValue typeVal = new (127);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE tinyint_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeSmallIntIntParam() returns error? {
    SmallIntValue typeVal = new (32767);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE smallint_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeBigIntIntParam() returns error? {
    BigIntValue typeVal = new (9223372036854774807);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE bigint_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeDoubleDoubleParam() returns error? {
    DoubleValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeDoubleIntParam() returns error? {
    DoubleValue typeVal = new (1234);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE float_type = ${typeVal}`;
    record{} returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertEquals(returnData.length(), 10);
    test:assertEquals(returnData["ID"], 2);
    test:assertEquals(returnData["REAL_TYPE"], 1234.0);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeDoubleDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    DoubleValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeFloatDoubleParam() returns error? {
    DoubleValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeRealDoubleParam() returns error? {
    RealValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE real_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeNumericDoubleParam() returns error? {
    NumericValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE numeric_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeNumericIntParam() returns error? {
    NumericValue typeVal = new (1234);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE numeric_type = ${typeVal}`;
    record{} returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertEquals(returnData.length(), 10);
    test:assertEquals(returnData["ID"], 2);
    test:assertEquals(returnData["REAL_TYPE"], 1234.0);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeNumericDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    NumericValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE numeric_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeDecimalDoubleParam() returns error? {
    DecimalValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE decimal_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeDecimalDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    DecimalValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * FROM NumericTypes WHERE decimal_type = ${typeVal}`;
    validateNumericTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordByteArrayParam() returns error? {
    record {} value = check queryRecordMockClient(queryRowDb, `SELECT * FROM ComplexTypes WHERE row_id = 1`);
    byte[] binaryData = <byte[]>getUntaintedData(value, "BINARY_TYPE");
    ParameterizedQuery sqlQuery = `SELECT * FROM ComplexTypes WHERE binary_type = ${binaryData}`;
    validateComplexTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeBinaryByteParam() returns error? {
    record {} value = check queryRecordMockClient(queryRowDb, `SELECT * FROM ComplexTypes where row_id = 1`);
    byte[] binaryData = <byte[]>getUntaintedData(value, "BINARY_TYPE");
    BinaryValue typeVal = new (binaryData);
    ParameterizedQuery sqlQuery = `SELECT * FROM ComplexTypes WHERE binary_type = ${typeVal}`;
    validateComplexTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeBinaryReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();
    BinaryValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * FROM ComplexTypes WHERE binary_type = ${typeVal}`;
    validateComplexTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeVarBinaryReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();
    VarBinaryValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * FROM ComplexTypes WHERE var_binary_type = ${typeVal}`;
    validateComplexTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeTinyBlobByteParam() returns error? {
    record {}|error? value = check queryMockClient(queryRowDb, `SELECT * from ComplexTypes where row_id = 1`);
    byte[] binaryData = <byte[]>getUntaintedData(value, "BLOB_TYPE");
    BinaryValue typeVal = new (binaryData);
    ParameterizedQuery sqlQuery = `SELECT * FROM ComplexTypes WHERE blob_type = ${typeVal}`;
    validateComplexTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeClobStringParam() returns error? {
    ClobValue typeVal = new ("very long text");
    ParameterizedQuery sqlQuery = `SELECT * FROM ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeClobReadableCharChannelParam() returns error? {
    io:ReadableCharacterChannel clobChannel = check getClobColumnChannel();
    ClobValue typeVal = new (clobChannel);
    ParameterizedQuery sqlQuery = `SELECT * FROM ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTypeNClobReadableCharChannelParam() returns error? {
    io:ReadableCharacterChannel clobChannel = check getClobColumnChannel();
    NClobValue typeVal = new (clobChannel);
    ParameterizedQuery sqlQuery = `SELECT * FROM ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDateStringParam() returns error? {
    DateValue typeVal = new ("2017-02-03");
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDateString2Param() returns error? {
    DateValue typeVal = new ("2017-2-3");
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDateStringInvalidParam() {
    DateValue typeVal = new ("2017/2/3");
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE date_type = ${typeVal}`;
    record{}|error result = trap queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertTrue(result is error);

    if result is ApplicationError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * FROM " +
                "DateTimeTypes WHERE date_type =  ? . java.lang.IllegalArgumentException"));
    } else {
        test:assertFail("ApplicationError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDateTimeRecordParam() returns error? {
    time:Date date = {year: 2017, month:2, day: 3};
    DateValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimestampWithTimeZoneRecordParam() returns error? {
    time:Civil dateTime = {utcOffset: {hours: +8, minutes: 0}, year:2008, month:8, day:8, hour: 20, minute: 8,
                            second:8};
    DateTimeValue typeVal = new (dateTime);
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE timestamp_tz_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimeStringParam() returns error? {
    TimeValue typeVal = new ("11:35:45");
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE time_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimeStringInvalidParam() {
    TimeValue typeVal = new ("11-35-45");
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE time_type = ${typeVal}`;
    record{}|error? result = trap queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertTrue(result is error);

    if result is DatabaseError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * FROM DateTimeTypes " +
        "WHERE time_type =  ? . data exception: invalid datetime format."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimeTimeRecordParam() returns error? {
    time:TimeOfDay date = {hour: 11, minute: 35, second:45};
    TimeValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE time_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimeTimeRecordWithTimeZoneParam() returns error? {
    time:TimeOfDay time = {utcOffset: {hours: -8, minutes: 0}, hour: 4, minute: 8, second: 8};
    TimeValue typeVal = new (time);
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE time_tz_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimestampStringParam() returns error? {
    TimestampValue typeVal = new ("2017-02-03 11:53:00");
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimestampStringInvalidParam() {
    TimestampValue typeVal = new ("2017/02/03 11:53:00");
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    record{}|error result = queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertTrue(result is error);

    if result is DatabaseError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * FROM DateTimeTypes " +
        "WHERE timestamp_type =  ? . data exception: invalid datetime format."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimestampTimeRecordParam() returns error? {
    time:Utc date = check time:utcFromString("2017-02-03T11:53:00.00Z");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimestampTimeRecordWithTimeZoneParam() returns error? {
    time:Utc date = check time:utcFromString("2017-02-03T11:53:00.00Z");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordDateTimeTimeRecordWithTimeZoneParam() returns error? {
    time:Civil date = {year: 2017, month:2, day: 3, hour: 11, minute: 53, second:0};
    DateTimeValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE datetime_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordTimestampTimeRecordWithTimeZone2Param() returns error? {
    time:Utc date = check time:utcFromString("2008-08-08T20:08:08+08:00");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * FROM DateTimeTypes WHERE timestamp_tz_type = ${typeVal}`;
    validateDateTimeTypesTableRecordResult(check queryRecordMockClient(queryRowDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-row"]
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
    `SELECT * FROM ArrayTypes WHERE int_array = ${paraInt}
                                AND long_array = ${paraLong}
                                AND float_array = ${paraFloat}
                                AND double_array = ${paraDouble}
                                AND decimal_array = ${paraDecimal}
                                AND string_array = ${paraString}
                                AND boolean_array = ${paraBool}`;
    record{} returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertEquals(returnData["INT_ARRAY"], [1, 2, 3]);
    test:assertEquals(returnData["LONG_ARRAY"], [10000, 20000, 30000]);
    test:assertEquals(returnData["BOOLEAN_ARRAY"], [true, false, true]);
    test:assertEquals(returnData["STRING_ARRAY"], ["Hello", "Ballerina"]);
    test:assertNotEquals(returnData["FLOAT_ARRAY"], ());
    test:assertNotEquals(returnData["DECIMAL_ARRAY"], ());
    test:assertNotEquals(returnData["DOUBLE_ARRAY"], ());
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordArrayBasicNullParams() returns error? {
    ParameterizedQuery sqlQuery =
        `SELECT * FROM ArrayTypes WHERE int_array is null AND long_array is null AND float_array
         is null AND double_array is null AND decimal_array is null AND string_array is null
         AND boolean_array is null`;

    record{} returnData = check queryRecordMockClient(queryRowDb, sqlQuery);
    test:assertEquals(returnData["INT_ARRAY"], ());
    test:assertEquals(returnData["LONG_ARRAY"], ());
    test:assertEquals(returnData["FLOAT_ARRAY"], ());
    test:assertEquals(returnData["DECIMAL_ARRAY"], ());
    test:assertEquals(returnData["DOUBLE_ARRAY"], ());
    test:assertEquals(returnData["BOOLEAN_ARRAY"], ());
    test:assertEquals(returnData["STRING_ARRAY"], ());
    test:assertEquals(returnData["BLOB_ARRAY"], ());
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordNoCheck() returns error? {
    int rowId = 1;
    MockClient dbClient = check getMockClient(queryRowDb);
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE row_id = ${rowId}`;
    record{}|error queryResult = dbClient->queryRow(sqlQuery);
    check dbClient.close();
    if queryResult is record{} {
        validateDataTableRecordResult(queryResult);
    } else {
        test:assertFail("Unexpected error");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordNegative1() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * FROM EmptyDataTable WHERE row_id = ${rowId}`;
    record {}|error queryResult = queryRecordMockClient(queryRowDb, sqlQuery);
    if queryResult is error {
        test:assertTrue(queryResult is NoRowsError, "Incorrect error type");
        test:assertTrue(queryResult.message().endsWith("Query did not retrieve any rows."), "Incorrect error message");
    } else {
        test:assertFail("Expected no rows error when querying empty table.");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordNegative2() returns error? {
    int rowId = 1;
    MockClient dbClient = check getMockClient(queryRowDb);
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE row_id = ${rowId}`;
    record{}|int|error queryResult = dbClient->queryRow(sqlQuery);
    if queryResult is error {
        test:assertEquals(queryResult.message(), "Return type cannot be a union of multiple types.");
    } else {
        test:assertFail("Expected error when querying with invalid column name.");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryRecordNoCheckNegative() returns error? {
    int rowId = 1;
    MockClient dbClient = check getMockClient(queryRowDb);
    ParameterizedQuery sqlQuery = `SELECT row_id, invalid_column_name FROM DataTable WHERE row_id = ${rowId}`;
    record{}|error queryResult = dbClient->queryRow(sqlQuery);
    check dbClient.close();
    if queryResult is error {
        test:assertTrue(queryResult.message().endsWith("user lacks privilege or object not found: INVALID_COLUMN_NAME in statement [SELECT row_id, invalid_column_name FROM DataTable WHERE row_id =  ? ]."),
                        "Incorrect error message");
    } else {
        test:assertFail("Expected error when querying with invalid column name.");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValue() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT COUNT(*) FROM DataTable";
    int count = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(count, 3);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueNegative1() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * FROM DataTable WHERE row_id = ${rowId}`;
    int|error queryResult = dbClient->queryRow(sqlQuery);
    check dbClient.close();
    if queryResult is error {
        test:assertTrue(queryResult is TypeMismatchError, "Incorrect error type");
        test:assertEquals(queryResult.message(), "Expected type to be 'int' but found 'record{}'");
    } else {
        test:assertFail("Expected error when query result contains multiple columns.");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueNegative2() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT string_type FROM DataTable WHERE row_id = ${rowId}`;
    int|error queryResult = dbClient->queryRow(sqlQuery);
    check dbClient.close();
    if queryResult is error {
        test:assertEquals(queryResult.message(),
                        "SQL Type 'Retrieved SQL type' cannot be converted to ballerina type 'int'.");
    } else {
        test:assertFail("Expected error when query returns unexpected result type.");
    }
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeInt() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT int_type FROM NumericTypes WHERE id = 1";
    int returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, 2147483647);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeFloat() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT float_type FROM NumericTypes WHERE id = 1";
    float returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, 1234.567);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeDecimal() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT decimal_type FROM NumericTypes WHERE id = 1";
    decimal returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    decimal decimalValue = 1234.567;
    test:assertEquals(returnValue, decimalValue);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeBigInt() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT bigint_type FROM NumericTypes WHERE id = 1";
    int returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, 9223372036854774807);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeSmallInt() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT smallint_type FROM NumericTypes WHERE id = 1";
    int returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, 32767);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeTinyInt() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT tinyint_type FROM NumericTypes WHERE id = 1";
    int returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, 127);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeBit() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT bit_type FROM NumericTypes WHERE id = 1";
    int returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, 1);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeNumeric() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT numeric_type FROM NumericTypes WHERE id = 1";
    decimal returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    decimal decimalValue = 1234.567;
    test:assertEquals(returnValue, decimalValue);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeString() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT string_type FROM DataTable WHERE row_id = 1";
    string returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, "Hello");
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeBoolean() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT boolean_type FROM DataTable WHERE row_id = 1";
    boolean returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, true);
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeBlob() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT blob_type FROM ComplexTypes WHERE row_id = 1";
    byte[] returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, "wso2 ballerina blob test.".toBytes());
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeClob() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT clob_type FROM ComplexTypes WHERE row_id = 1";
    string returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, "very long text");
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryValueTypeBinary() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    string sqlQuery = "SELECT binary_type FROM ComplexTypes WHERE row_id = 1";
    byte[] returnValue = check dbClient->queryRow(sqlQuery);
    check dbClient.close();
    test:assertEquals(returnValue, "wso2 ballerina binary test.".toBytes());
}

@test:Config {
    groups: ["query", "query-row"]
}
function testGetPrimitiveTypesRecord() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    SelectTestAlias value = check dbClient->queryRow(
	    `SELECT int_type, long_type, double_type, boolean_type, string_type FROM DataTable WHERE row_id = 1`);
    check dbClient.close();

    SelectTestAlias expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        double_type: 2139095039,
        boolean_type: true,
        string_type: "Hello"
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testGetPrimitiveTypesLessFieldsRecord() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    SelectTestAlias2 value = check dbClient->queryRow(
	    `SELECT int_type, long_type, double_type, boolean_type, string_type FROM DataTable WHERE row_id = 1`);
    check dbClient.close();

    var expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        double_type: 2.139095039E9,
        BOOLEAN_TYPE: true,
        STRING_TYPE: "Hello"
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testComplexTypesNilRecord() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    record{} value = check dbClient->queryRow(
        `SELECT blob_type, clob_type, binary_type, other_type, uuid_type FROM ComplexTypes WHERE row_id = 2`);
    check dbClient.close();
    var complexStringType = {
        blob_type: (),
        clob_type: (),
        binary_type: (),
        other_type: (),
        uuid_type: ()
    };
    test:assertEquals(value, complexStringType, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testArrayRetrievalRecord() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    record{} value = check dbClient->queryRow(`SELECT int_type, int_array, long_type, long_array, boolean_type,
        string_type, string_array, boolean_array FROM MixTypes WHERE row_id = 1`);
    check dbClient.close();

    float[] doubleTypeArray = [245.23, 5559.49, 8796.123];
    var mixTypesExpected = {
        INT_TYPE: 1,
        INT_ARRAY: [1, 2, 3],
        LONG_TYPE:9223372036854774807,
        LONG_ARRAY: [100000000, 200000000, 300000000],
        BOOLEAN_TYPE: true,
        STRING_TYPE: "Hello",
        STRING_ARRAY: ["Hello", "Ballerina"],
        BOOLEAN_ARRAY: [true, false, true]
    };
    test:assertEquals(value, mixTypesExpected, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testComplexWithStructDefRecord() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    record{} value = check dbClient->queryRow(`SELECT int_type, int_array, long_type, long_array, boolean_type,
        string_type, boolean_array, string_array, json_type FROM MixTypes WHERE row_id = 1`, TestTypeData);
    check dbClient.close();
    TestTypeData mixTypesExpected = {
        int_type: 1,
        int_array: [1, 2, 3],
        long_type: 9223372036854774807,
        long_array:[100000000, 200000000, 300000000],
        boolean_type: true,
        string_type: "Hello",
        boolean_array: [true, false, true],
        string_array: ["Hello", "Ballerina"],
        json_type: [1, 2, 3]
    };
    test:assertEquals(value, mixTypesExpected, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testDateTimeRecord() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    ResultDates value = check dbClient->queryRow(`SELECT date_type, time_type, timestamp_type, datetime_type,
        time_tz_type, timestamp_tz_type FROM DateTimeTypes WHERE row_id = 1`);
    check dbClient.close();

    string dateTypeString = "2017-02-03";
    string timeTypeString = "11:35:45";
    string timestampTypeString = "2017-02-03 11:53:00.0";
    string timeWithTimezone = "20:08:08-08:00";
    string timestampWithTimezone = "2008-08-08T20:08:08+08:00";

    ResultDates expected = {
        date_type: dateTypeString,
        time_type: timeTypeString,
        timestamp_type: timestampTypeString,
        datetime_type: timestampTypeString,
        time_tz_type: timeWithTimezone,
        timestamp_tz_type: timestampWithTimezone
    };
    test:assertEquals(value, expected, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testDateTimeRecord2() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    ResultDates2 value = check dbClient->queryRow(`SELECT date_type, time_type, timestamp_type, datetime_type,
        time_tz_type, timestamp_tz_type FROM DateTimeTypes WHERE row_id = 1`);
    check dbClient.close();

    time:Date dateTypeRecord = {year: 2017, month: 2, day: 3};
    time:TimeOfDay timeTypeRecord = {hour: 11, minute: 35, second:45};
    time:Civil timestampTypeRecord = {year: 2017, month: 2, day: 3, hour: 11, minute: 53, second: 0};
    time:TimeOfDay timeWithTimezone = {utcOffset: {hours: -8, minutes: 0}, hour: 20, minute: 8, second: 8, "timeAbbrev": "-08:00"};
    time:Civil timestampWithTimezone = {utcOffset: {hours: 8, minutes: 0}, timeAbbrev: "+08:00", year:2008,
                                        month:8, day:8, hour: 20, minute: 8, second:8};

    ResultDates2 expected = {
        date_type: dateTypeRecord,
        time_type: timeTypeRecord,
        timestamp_type: timestampTypeRecord,
        datetime_type: timestampTypeRecord,
        time_tz_type: timeWithTimezone,
        timestamp_tz_type: timestampWithTimezone
    };
    test:assertEquals(value, expected, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testDateTimeRecord3() returns error? {
    record{}|error? result;
    MockClient dbClient = check getMockClient(queryRowDb);

    result = dbClient->queryRow(`SELECT date_type FROM DateTimeTypes where row_id = 1`, ResultDates3);
    test:assertTrue(result is error, "Error Expected for Date type.");
    test:assertEquals((<error>result).message(),
        "The ballerina type expected for 'SQL Date' type is 'time:Date' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    result = dbClient->queryRow(`SELECT time_type FROM DateTimeTypes where row_id = 1`, ResultDates3);
    test:assertTrue(result is error, "Error Expected for Time type.");
    test:assertEquals((<error>result).message(),
        "The ballerina type expected for 'SQL Time' type is 'time:TimeOfDay' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    result = dbClient->queryRow(`SELECT timestamp_type FROM DateTimeTypes where row_id = 1`, ResultDates3);
    test:assertTrue(result is error, "Error Expected for Timestamp type.");
    test:assertEquals((<error>result).message(),
        "The ballerina type expected for 'SQL Timestamp' type is 'time:Civil' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    result = dbClient->queryRow(`SELECT datetime_type FROM DateTimeTypes where row_id = 1`, ResultDates3);
    test:assertTrue(result is error, "Error Expected for Datetime type.");
    test:assertEquals((<error>result).message(),
        "The ballerina type expected for 'SQL Timestamp' type is 'time:Civil' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    result = dbClient->queryRow(`SELECT time_tz_type FROM DateTimeTypes where row_id = 1`, ResultDates3);
    test:assertTrue(result is error, "Error Expected for Time with Timezone type.");
    test:assertEquals((<error>result).message(),
        "The ballerina type expected for 'SQL Time with Timezone' type is 'time:TimeOfDay' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    result = dbClient->queryRow(`SELECT timestamp_tz_type FROM DateTimeTypes where row_id = 1`, ResultDates3);
    test:assertTrue(result is error, "Error Expected for Timestamp with Timezone type.");
    test:assertEquals((<error>result).message(),
        "The ballerina type expected for 'SQL Timestamp with Timezone' type is 'time:Civil' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    check dbClient.close();
}

@test:Config {
    groups: ["query", "query-row"]
}
function testGetArrayTypesRecord() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    ArrayRecord value = check dbClient->queryRow(`SELECT row_id,smallint_array, int_array, long_array, float_array,
        double_array, decimal_array, real_array,numeric_array, varchar_array, char_array, nvarchar_array, boolean_array,
        bit_array, date_array, time_array, datetime_array, timestamp_array, blob_array, time_tz_array,
        timestamp_tz_array FROM ArrayTypes2 WHERE row_id = 1`);
    check dbClient.close();
    ArrayRecord expectedData = {
        row_id: 1,
        blob_array: [<byte[]>[119,115,111,50,32,98,97,108,108,101,114,105,110,97,32,98,108,111,98,32,116,101,115,116,46],
                    <byte[]>[119,115,111,50,32,98,97,108,108,101,114,105,110,97,32,98,108,111,98,32,116,101,115,116,46]],
        smallint_array: [12, 232],
        int_array: [1, 2, 3],
        long_array: [100000000, 200000000, 300000000],
        float_array: [245.23, 5559.49, 8796.123],
        double_array: [245.23, 5559.49, 8796.123],
        decimal_array: [245.12, 5559.12, 8796.92],
        real_array: [199.33,2399.1],
        numeric_array: [11.11, 23.23],
        varchar_array: ["Hello", "Ballerina"],
        char_array: ["Hello          ", "Ballerina      "],
        nvarchar_array: ["Hello", "Ballerina"],
        boolean_array: [true, false, true],
        bit_array: [<byte[]>[32], <byte[]>[96], <byte[]>[128]],
        date_array: [<time:Date>{year: 2017, month: 2, day: 3}, <time:Date>{year: 2017, month: 2, day: 3}],
        time_array: [<time:TimeOfDay>{hour: 11, minute: 22, second: 42}, <time:TimeOfDay>{hour: 12, minute: 23, second: 45}],
        datetime_array: [<time:Civil>{year: 2017, month: 2, day: 3, hour: 11, minute: 53, second: 0}, <time:Civil>{year: 2019, month: 4, day: 5, hour: 12, minute: 33, second: 10}],
        timestamp_array: [<time:Civil>{year: 2017, month: 2, day: 3, hour: 11, minute: 53, second: 0}, <time:Civil>{year: 2019, month: 4, day: 5, hour: 12, minute: 33, second: 10}],
        time_tz_array: [<time:TimeOfDay>{utcOffset: {hours: 6, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+06:30"}, <time:TimeOfDay>{utcOffset: {hours: 4, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+04:30"}],
        timestamp_tz_array: [<time:Civil>{utcOffset: {hours: -8, minutes: 0}, timeAbbrev: "-08:00", year:2017, month:1, day:25, hour: 16, minute: 33, second:55}, <time:Civil>{utcOffset: {hours: -5, minutes: 0}, timeAbbrev: "-05:00", year:2017, month:1, day:25, hour: 16, minute: 33, second:55}]
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}


@test:Config {
    groups: ["query", "query-row"]
}
function testGetArrayTypesRecord2() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    ArrayRecord value = check dbClient->queryRow(`SELECT row_id,smallint_array, int_array, long_array, float_array,
        double_array, decimal_array, real_array,numeric_array, varchar_array, char_array, nvarchar_array, boolean_array,
        bit_array, date_array, time_array, datetime_array, timestamp_array, blob_array, time_tz_array,
        timestamp_tz_array FROM ArrayTypes2 WHERE row_id = 2`);
    check dbClient.close();
    ArrayRecord expectedData = {
        row_id: 2,
        blob_array: [null, null],
        smallint_array: [null, null],
        int_array: [null, null],
        long_array: [null, null],
        float_array: [null, null],
        double_array: [null, null],
        decimal_array: [null, null],
        real_array: [null, null],
        numeric_array: [null, null],
        varchar_array:[null, null],
        char_array: [null, null],
        nvarchar_array: [null, null],
        boolean_array: [null, null],
        bit_array: [null, null],
        date_array: [null, null],
        time_array: [null, null],
        datetime_array: [null, null],
        timestamp_array: [null, null],
        time_tz_array: [null, null],
        timestamp_tz_array: [null, null]
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testGetArrayTypesRecord3() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    ArrayRecord value = check dbClient->queryRow(`SELECT row_id,smallint_array, int_array, long_array, float_array,
        double_array, decimal_array, real_array,numeric_array, varchar_array, char_array, nvarchar_array, boolean_array,
        bit_array, date_array, time_array, datetime_array, timestamp_array, blob_array, time_tz_array,
        timestamp_tz_array FROM ArrayTypes2 WHERE row_id = 3`);
    check dbClient.close();
    ArrayRecord expectedData = {
        row_id: 3,
        blob_array: [null, <byte[]>[119,115,111,50,32,98,97,108,108,101,114,105,110,97,32,98,108,111,98,32,116,101,115,116,46],
                    <byte[]>[119,115,111,50,32,98,97,108,108,101,114,105,110,97,32,98,108,111,98,32,116,101,115,116,46]],
        smallint_array: [null, 12, 232],
        int_array: [null, 1, 2, 3],
        long_array: [null, 100000000, 200000000, 300000000],
        float_array: [null, 245.23, 5559.49, 8796.123],
        double_array: [null, 245.23, 5559.49, 8796.123],
        decimal_array: [null, 245.12, 5559.12, 8796.92],
        real_array: [null, 199.33,2399.1],
        numeric_array: [null, 11.11, 23.23],
        varchar_array: [null, "Hello", "Ballerina"],
        char_array: [null, "Hello          ", "Ballerina      "],
        nvarchar_array: [null, "Hello", "Ballerina"],
        boolean_array: [null, true, false, true],
        bit_array: [null, <byte[]>[32], <byte[]>[96], <byte[]>[128]],
        date_array: [null, <time:Date>{year: 2017, month: 2, day: 3}, <time:Date>{year: 2017, month: 2, day: 3}],
        time_array: [null, <time:TimeOfDay>{hour: 11, minute: 22, second: 42}, <time:TimeOfDay>{hour: 12, minute: 23, second: 45}],
        datetime_array: [null, <time:Civil>{year: 2017, month: 2, day: 3, hour: 11, minute: 53, second: 0}, <time:Civil>{year: 2019, month: 4, day: 5, hour: 12, minute: 33, second: 10}],
        timestamp_array: [null, <time:Civil>{year: 2017, month: 2, day: 3, hour: 11, minute: 53, second: 0}, <time:Civil>{year: 2019, month: 4, day: 5, hour: 12, minute: 33, second: 10}],
        time_tz_array: [null, <time:TimeOfDay>{utcOffset: {hours: 6, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+06:30"}, <time:TimeOfDay>{utcOffset: {hours: 4, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+04:30"}],
        timestamp_tz_array: [null, <time:Civil>{utcOffset: {hours: -8, minutes: 0}, timeAbbrev: "-08:00", year:2017, month:1, day:25, hour: 16, minute: 33, second:55}, <time:Civil>{utcOffset: {hours: -5, minutes: 0}, timeAbbrev: "-05:00", year:2017, month:1, day:25, hour: 16, minute: 33, second:55}]
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

@test:Config {
    groups: ["query", "query-row"]
}
function testGetArrayTypesRecord4() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    ArrayRecord value = check dbClient->queryRow(`SELECT row_id,smallint_array, int_array, long_array, float_array,
        double_array, decimal_array, real_array,numeric_array, varchar_array, char_array, nvarchar_array, boolean_array,
        bit_array, date_array, time_array, datetime_array, timestamp_array, blob_array, time_tz_array,
        timestamp_tz_array FROM ArrayTypes2 WHERE row_id = 4`);
    check dbClient.close();
    ArrayRecord expectedData = {
        row_id: 4,
        blob_array: (),
        smallint_array: (),
        int_array: (),
        long_array: (),
        float_array: (),
        double_array: (),
        decimal_array: (),
        real_array: (),
        numeric_array: (),
        varchar_array:(),
        char_array: (),
        nvarchar_array: (),
        boolean_array: (),
        bit_array: (),
        date_array: (),
        time_array: (),
        datetime_array: (),
        timestamp_array: (),
        time_tz_array: (),
        timestamp_tz_array: ()
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

isolated function validateDataTableRecordResult(record{}? returnData) {
    decimal decimalVal = 23.45;
    if returnData is () {
        test:assertFail("Empty row returned.");
    } else {
        test:assertEquals(returnData["ROW_ID"], 1);
        test:assertEquals(returnData["INT_TYPE"], 1);
        test:assertEquals(returnData["LONG_TYPE"], 9223372036854774807);
        test:assertEquals(returnData["DOUBLE_TYPE"], <float> 2139095039);
        test:assertEquals(returnData["BOOLEAN_TYPE"], true);
        test:assertEquals(returnData["DECIMAL_TYPE"], decimalVal);
        test:assertEquals(returnData["STRING_TYPE"], "Hello");
        test:assertTrue(returnData["FLOAT_TYPE"] is float);   
    } 
}

isolated function validateNumericTableRecordResult(record{}? returnData) {
    if returnData is () {
        test:assertFail("Empty row returned.");
    } else {
        test:assertEquals(returnData["ID"], 1);
        test:assertEquals(returnData["INT_TYPE"], 2147483647);
        test:assertEquals(returnData["BIGINT_TYPE"], 9223372036854774807);
        test:assertEquals(returnData["SMALLINT_TYPE"], 32767);
        test:assertEquals(returnData["TINYINT_TYPE"], 127);
        test:assertEquals(returnData["BIT_TYPE"], true);
        test:assertTrue(returnData["REAL_TYPE"] is float);
        test:assertTrue(returnData["DECIMAL_TYPE"] is decimal);
        test:assertTrue(returnData["NUMERIC_TYPE"] is decimal);
        test:assertTrue(returnData["FLOAT_TYPE"] is float);
    }
}

isolated function validateComplexTableRecordResult(record{}? returnData) {
    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 7);
        test:assertEquals(returnData["ROW_ID"], 1);
        test:assertEquals(returnData["CLOB_TYPE"], "very long text");
    }
}

isolated function validateDateTimeTypesTableRecordResult(record{}? returnData) {
    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 7);
        test:assertEquals(returnData["ROW_ID"], 1);
        test:assertTrue(returnData["DATE_TYPE"].toString().startsWith("2017-02-03"));
    }
}
