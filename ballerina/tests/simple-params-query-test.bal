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
import ballerina/time;
import ballerina/test;

string simpleParamsDb = urlPrefix + "9010/querysimpleparams";
boolean initSimpleParams = false;
boolean cleanSimpleParams = false;

@test:BeforeGroups {
	value: ["query-simple-params"]	
} 
function initQueryParamsContainer() returns error? {
	check initializeDockerContainer("sql-query-params", "querysimpleparams", "9010", "query", "simple-params-test-data.sql");
}

@test:AfterGroups {
    value: ["query-simple-params"]
}
function cleanQueryParamsContainer() returns error? {
	check cleanDockerContainer("sql-query-params");
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function querySingleIntParam() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleIntParam() returns error? {
    int rowId = 1;
    int intType = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId} AND int_type =  ${intType}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryIntAndLongParam() returns error? {
    int rowId = 1;
    int longType = 9223372036854774807;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId} AND long_type = ${longType}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryStringParam() returns error? {
    string stringType = "Hello";
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${stringType}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryIntAndStringParam() returns error? {
    string stringType = "Hello";
    int rowId =1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${stringType} AND row_id = ${rowId}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleParam() returns error? {
    float doubleType = 2139095039.0;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE double_type = ${doubleType}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryFloatParam() returns error? {
    float floatType = 123.34;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE float_type = ${floatType}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleAndFloatParam() returns error? {
    float floatType = 123.34;
    float doubleType = 2139095039.0;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE float_type = ${floatType}
                                                                    and double_type = ${doubleType}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDecimalParam() returns error? {
    decimal decimalValue = 23.45;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE decimal_type = ${decimalValue}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDecimalAnFloatParam() returns error? {
    decimal decimalValue = 23.45;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE decimal_type = ${decimalValue}
                                                                    and double_type = 2139095039.0`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeVarcharStringParam() returns error? {
    VarcharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeCharStringParam() returns error? {
    CharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNCharStringParam() returns error? {
    NCharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNVarCharStringParam() returns error? {
    NVarcharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeVarCharIntegerParam() returns error? {
    int intVal = 1;
    NCharValue typeVal = new (intVal.toString());
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;

    decimal decimalVal = 25.45;
    record {}? returnData = check queryMockClient(simpleParamsDb, sqlQuery);
    test:assertNotEquals(returnData, ());
    if returnData is () {
        test:assertFail("Query returns ()");
    } else {
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
    groups: ["query", "query-simple-params"]
}
function queryTypBooleanBooleanParam() returns error? {
    BooleanValue typeVal = new (true);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBitIntParam() returns error? {
    BitValue typeVal = new (1);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBitStringParam() returns error? {
    BitValue typeVal = new (true);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBitInvalidIntParam() {
    BitValue typeVal = new (12);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    record{}|error? returnVal = trap queryMockClient(simpleParamsDb, sqlQuery);
    test:assertTrue(returnVal is error);
    error dbError = <error> returnVal;
    test:assertEquals(dbError.message(), "Only 1 or 0 can be passed for BitValue SQL Type, but found :12");
}

type DataTableRecord record {
    float int_type;
};

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryIntTypeInvalidParam() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT int_type from DataTable WHERE row_id = ${rowId}`;
    MockClient dbClient = check new (url = simpleParamsDb, user = user, password = password);
    stream<DataTableRecord, error?> streamData = dbClient->query(sqlQuery);
    record {|DataTableRecord value;|}|error? data = streamData.next();
    check streamData.close();
    check dbClient.close();
    test:assertTrue(data is error);
    if data is ApplicationError {
        test:assertTrue(data.message().startsWith("The field 'int_type' of type float cannot be mapped to the column " +
        "'INT_TYPE' of SQL type 'INTEGER'"));
    } else {
        test:assertFail("ApplicationError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeIntIntParam() returns error? {
    IntegerValue typeVal = new (2147483647);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE int_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeTinyIntIntParam() returns error? {
    SmallIntValue typeVal = new (127);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE tinyint_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeSmallIntIntParam() returns error? {
    SmallIntValue typeVal = new (32767);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE smallint_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBigIntIntParam() returns error? {
    BigIntValue typeVal = new (9223372036854774807);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE bigint_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDoubleDoubleParam() returns error? {
    DoubleValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDoubleIntParam() returns error? {
    DoubleValue typeVal = new (1234);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    record{}? returnData = check queryMockClient(simpleParamsDb, sqlQuery);

    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 10);
        test:assertEquals(returnData["ID"], 2);
        test:assertEquals(returnData["REAL_TYPE"], 1234.0);
    }

}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDoubleDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    DoubleValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeFloatDoubleParam() returns error? {
    DoubleValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeRealDoubleParam() returns error? {
    RealValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE real_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNumericDoubleParam() returns error? {
    NumericValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNumericIntParam() returns error? {
    NumericValue typeVal = new (1234);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    record{}? returnData = check queryMockClient(simpleParamsDb, sqlQuery);

    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 10);
        test:assertEquals(returnData["ID"], 2);
        test:assertEquals(returnData["REAL_TYPE"], 1234.0);
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNumericDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    NumericValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDecimalDoubleParam() returns error? {
    DecimalValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE decimal_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDecimalDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    DecimalValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE decimal_type = ${typeVal}`;
    validateNumericTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryByteArrayParam() returns error? {
    record {}|error? value = check queryMockClient(simpleParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BINARY_TYPE");
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${binaryData}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBinaryByteParam() returns error? {
    record {}|error? value = check queryMockClient(simpleParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BINARY_TYPE");
    BinaryValue typeVal = new (binaryData);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${typeVal}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBinaryReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();
    BinaryValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${typeVal}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeVarBinaryReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();
    VarBinaryValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE var_binary_type = ${typeVal}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeTinyBlobByteParam() returns error? {
    record {}|error? value = check queryMockClient(simpleParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BLOB_TYPE");
    BinaryValue typeVal = new (binaryData);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE blob_type = ${typeVal}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBlobReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getBlobColumnChannel();
    BlobValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE blob_type = ${typeVal}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeClobStringParam() returns error? {
    ClobValue typeVal = new ("very long text");
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeClobReadableCharChannelParam() returns error? {
    io:ReadableCharacterChannel clobChannel = check getClobColumnChannel();
    ClobValue typeVal = new (clobChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNClobReadableCharChannelParam() returns error? {
    io:ReadableCharacterChannel clobChannel = check getClobColumnChannel();
    NClobValue typeVal = new (clobChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateStringParam() returns error? {
    DateValue typeVal = new ("2017-02-03");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateString2Param() returns error? {
    DateValue typeVal = new ("2017-2-3");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateStringInvalidParam() {
    DateValue typeVal = new ("2017/2/3");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    record{}|error? result = trap queryMockClient(simpleParamsDb, sqlQuery);
    test:assertTrue(result is error);

    if result is ApplicationError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from " +
                "DateTimeTypes WHERE date_type =  ? . java.lang.IllegalArgumentException"));
    } else {
        test:assertFail("ApplicationError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateTimeRecordParam() returns error? {
    time:Date date = {year: 2017, month:2, day: 3};
    DateValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampWithTimeZoneRecordParam() returns error? {
    time:Civil dateTime = {utcOffset: {hours: +8, minutes: 0}, year:2008, month:8, day:8, hour: 20, minute: 8,
                            second:8};
    DateTimeValue typeVal = new (dateTime);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type2 = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimeStringParam() returns error? {
    TimeValue typeVal = new ("11:35:45");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimeStringInvalidParam() {
    TimeValue typeVal = new ("11-35-45");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    record{}|error? result = trap queryMockClient(simpleParamsDb, sqlQuery);
    test:assertTrue(result is error);

    if result is DatabaseError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from DateTimeTypes " +
        "WHERE time_type =  ? . data exception: invalid datetime format."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimeTimeRecordParam() returns error? {
    time:TimeOfDay date = {hour: 11, minute: 35, second:45};
    TimeValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimeTimeRecordWithTimeZoneParam() returns error? {
    time:TimeOfDay time = {utcOffset: {hours: -8, minutes: 0}, hour: 4, minute: 8, second: 8};
    TimeValue typeVal = new (time);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type2 = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampStringParam() returns error? {
    TimestampValue typeVal = new ("2017-02-03 11:53:00");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampStringInvalidParam() {
    TimestampValue typeVal = new ("2017/02/03 11:53:00");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    record{}|error? result = trap queryMockClient(simpleParamsDb, sqlQuery);
    test:assertTrue(result is error);

    if result is DatabaseError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from DateTimeTypes " +
        "WHERE timestamp_type =  ? . data exception: invalid datetime format."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampTimeRecordParam() returns error? {
    time:Utc date = check time:utcFromString("2017-02-03T11:53:00.00Z");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampTimeRecordWithTimeZoneParam() returns error? {
    time:Utc date = check time:utcFromString("2017-02-03T11:53:00.00Z");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateTimeTimeRecordWithTimeZoneParam() returns error? {
    time:Civil date = {year: 2017, month:2, day: 3, hour: 11, minute: 53, second:0};
    DateTimeValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE datetime_type = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampTimeRecordWithTimeZone2Param() returns error? {
    time:Utc date = check time:utcFromString("2008-08-08T20:08:08+08:00");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type2 = ${typeVal}`;
    validateDateTimeTypesTableResult(check queryMockClient(simpleParamsDb, sqlQuery));
}

//Can not test the SQL type TEXT with hsqldb
@test:Config {
    groups: ["query", "query-simple-params"]
}
isolated function testCreatingTextValue() returns error? {
    TextValue textValue = new("Text Value Field");
    test:assertTrue(textValue.value is string);
}

//Can not test SQL type STRUCT with hsqldb
@test:Config {
    groups: ["query", "query-simple-params"]
}
isolated function testCreatingStuctValue() returns error? {
    StructValue structValue = new({"key":"value"});
    test:assertTrue(structValue.value is record {});
}

//Can not test SQL type REF with hsqldb
@test:Config {
    groups: ["query", "query-simple-params"]
}
isolated function testCreatingRefValue() returns error? {
    RefValue refValue = new({"key":"value"});
    test:assertTrue(refValue.value is record {});
}

//Can not test SQL type ROW with hsqldb
@test:Config {
    groups: ["query", "query-simple-params"]
}
isolated function testCreatingRowValue() returns error? {
    RowValue rowValue = new([1, 2]);
    test:assertTrue(rowValue.value is byte[]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryArrayBasicParams() returns error? {
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
    record{}? returnData = check queryMockClient(simpleParamsDb, sqlQuery);
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
    groups: ["query", "query-simple-params"]
}
function queryArrayBasicNullParams() returns error? {
    ParameterizedQuery sqlQuery =
        `SELECT * from ArrayTypes WHERE int_array is null AND long_array is null AND float_array
         is null AND double_array is null AND decimal_array is null AND string_array is null
         AND boolean_array is null`;

    record{}? returnData = check queryMockClient(simpleParamsDb, sqlQuery);
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

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryRecord() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId}`;
    validateDataTableResult(check queryRecordMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryRecordNegative1() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * from EmptyDataTable WHERE row_id = ${rowId}`;
    record {}|error queryResult = queryRecordMockClient(simpleParamsDb, sqlQuery);
    if queryResult is error {
        test:assertTrue(queryResult is NoRowsError, "Incorrect error type");
        test:assertTrue(queryResult.message().endsWith("Query did not retrieve any rows."), "Incorrect error message");
    } else {
        test:assertFail("Expected no rows error when querying empty table.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryRecordNegative2() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT row_id, invalid_column_name from DataTable WHERE row_id = ${rowId}`;
    record {}|error queryResult = queryRecordMockClient(simpleParamsDb, sqlQuery);
    if queryResult is error {
        io:println(queryResult.message());
        test:assertTrue(queryResult.message().endsWith("user lacks privilege or object not found: INVALID_COLUMN_NAME in statement [SELECT row_id, invalid_column_name from DataTable WHERE row_id =  ? ]."),
                        "Incorrect error message");
    } else {
        test:assertFail("Expected error when querying with invalid column name.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryValue() returns error? {
    ParameterizedQuery sqlQuery = `SELECT COUNT(*) from DataTable`;
    int count = check queryValueMockClient(simpleParamsDb, sqlQuery);
    test:assertEquals(count, 3);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryValueNegative() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId}`;
    int|error queryResult = queryValueMockClient(simpleParamsDb, sqlQuery);
    if queryResult is error {
        test:assertTrue(queryResult is TypeMismatchError, "Incorrect error type");
        test:assertEquals(queryResult.message(), "Expected type to be 'int' but found 'record{}'");
    } else {
        test:assertFail("Expected error when query result contains multiple columns.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryValue2() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT row_id from DataTable`;
    int queryResult = check queryValueMockClient(simpleParamsDb, sqlQuery);
    test:assertEquals(queryResult, 1);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryValueNegative3() returns error? {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT string_type from DataTable WHERE row_id = ${rowId}`;
    int|error queryResult = queryValueMockClient(simpleParamsDb, sqlQuery);
    if queryResult is error {
        test:assertTrue(queryResult.message().endsWith("Retrieved SQL type field cannot be converted to ballerina type : int"),
                                                       "Incorrect error message");
    } else {
        test:assertFail("Expected error when query returns unexpected result type.");
    }
}

isolated function validateDataTableResult(record{}? returnData) {
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

isolated function validateNumericTableResult(record{}? returnData) {
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

isolated function validateComplexTableResult(record{}? returnData) {
    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 5);
        test:assertEquals(returnData["ROW_ID"], 1);
        test:assertEquals(returnData["CLOB_TYPE"], "very long text");
    }
}

isolated function validateDateTimeTypesTableResult(record{}? returnData) {
    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 7);
        test:assertEquals(returnData["ROW_ID"], 1);
        test:assertTrue(returnData["DATE_TYPE"].toString().startsWith("2017-02-03"));
    }
}

isolated function validateEnumTable(record{}? returnData) {
    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 2);
        test:assertEquals(returnData["ID"], 1);
        test:assertEquals(returnData["ENUM_TYPE"].toString(), "doctor");
    }
}

isolated function validateGeoTable(record{}? returnData) {
    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 2);
        test:assertEquals(returnData["ID"], 1);
        test:assertEquals(returnData["GEOM"].toString(), "POINT (7 52)");
    }
}

isolated function validateJsonTable(record{}? returnData) {
    if returnData is () {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 2);
        test:assertEquals(returnData["ID"], 1);
        test:assertEquals(returnData["JSON_TYPE"], "{\"id\": 100, \"name\": \"Joe\", \"groups\": \"[2,5]\"}");
    }
}
