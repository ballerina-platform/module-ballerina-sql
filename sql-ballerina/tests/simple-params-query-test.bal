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
function initQueryParamsContainer() {
	initializeDockerContainer("sql-query-params", "querysimpleparams", "9010", "query", "simple-params-test-data.sql");
}

@test:AfterGroups {
    value: ["query-simple-params"]
}
function cleanQueryParamsContainer() {
	cleanDockerContainer("sql-query-params");
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function querySingleIntParam() {
    int rowId = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleIntParam() {
    int rowId = 1;
    int intType = 1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId} AND int_type =  ${intType}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryIntAndLongParam() {
    int rowId = 1;
    int longType = 9223372036854774807;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE row_id = ${rowId} AND long_type = ${longType}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryStringParam() {
    string stringType = "Hello";
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${stringType}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryIntAndStringParam() {
    string stringType = "Hello";
    int rowId =1;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${stringType} AND row_id = ${rowId}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleParam() {
    float doubleType = 2139095039.0;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE double_type = ${doubleType}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryFloatParam() {
    float floatType = 123.34;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE float_type = ${floatType}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleAndFloatParam() {
    float floatType = 123.34;
    float doubleType = 2139095039.0;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE float_type = ${floatType}
                                                                    and double_type = ${doubleType}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDecimalParam() {
    decimal decimalValue = 23.45;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE decimal_type = ${decimalValue}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDecimalAnFloatParam() {
    decimal decimalValue = 23.45;
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE decimal_type = ${decimalValue}
                                                                    and double_type = 2139095039.0`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeVarcharStringParam() {
    VarcharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeCharStringParam() {
    CharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNCharStringParam() {
    NCharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNVarCharStringParam() {
    NVarcharValue typeVal = new ("Hello");
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeVarCharIntegerParam() {
    int intVal = 1;
    NCharValue typeVal = new (intVal.toString());
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE string_type = ${typeVal}`;

    decimal decimalVal = 25.45;
    record {}? returnData = queryMockClient(simpleParamsDb, sqlQuery);
    test:assertNotEquals(returnData, ());
    if (returnData is ()) {
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
function queryTypBooleanBooleanParam() {
    BooleanValue typeVal = new (true);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBitIntParam() {
    BitValue typeVal = new (1);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBitStringParam() {
    BitValue typeVal = new (true);
    ParameterizedQuery sqlQuery = `SELECT * from DataTable WHERE boolean_type = ${typeVal}`;
    validateDataTableResult(queryMockClient(simpleParamsDb, sqlQuery));
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

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeIntIntParam() {
    IntegerValue typeVal = new (2147483647);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE int_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeTinyIntIntParam() {
    SmallIntValue typeVal = new (127);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE tinyint_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeSmallIntIntParam() {
    SmallIntValue typeVal = new (32767);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE smallint_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBigIntIntParam() {
    BigIntValue typeVal = new (9223372036854774807);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE bigint_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDoubleDoubleParam() {
    DoubleValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDoubleIntParam() {
    DoubleValue typeVal = new (1234);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    record{}? returnData = queryMockClient(simpleParamsDb, sqlQuery);

    if (returnData is ()) {
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
function queryTypeDoubleDecimalParam() {
    decimal decimalVal = 1234.567;
    DoubleValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeFloatDoubleParam() {
    DoubleValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE float_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeRealDoubleParam() {
    RealValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE real_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNumericDoubleParam() {
    NumericValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNumericIntParam() {
    NumericValue typeVal = new (1234);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    record{}? returnData = queryMockClient(simpleParamsDb, sqlQuery);

    if (returnData is ()) {
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
function queryTypeNumericDecimalParam() {
    decimal decimalVal = 1234.567;
    NumericValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE numeric_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDecimalDoubleParam() {
    DecimalValue typeVal = new (1234.567);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE decimal_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeDecimalDecimalParam() {
    decimal decimalVal = 1234.567;
    DecimalValue typeVal = new (decimalVal);
    ParameterizedQuery sqlQuery = `SELECT * from NumericTypes WHERE decimal_type = ${typeVal}`;
    validateNumericTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryByteArrayParam() {
    record {}|error? value = queryMockClient(simpleParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BINARY_TYPE");
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${binaryData}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBinaryByteParam() {
    record {}|error? value = queryMockClient(simpleParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BINARY_TYPE");
    BinaryValue typeVal = new (binaryData);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${typeVal}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBinaryReadableByteChannelParam() {
    io:ReadableByteChannel byteChannel = getByteColumnChannel();
    BinaryValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE binary_type = ${typeVal}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeVarBinaryReadableByteChannelParam() {
    io:ReadableByteChannel byteChannel = getByteColumnChannel();
    VarBinaryValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE var_binary_type = ${typeVal}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeTinyBlobByteParam() {
    record {}|error? value = queryMockClient(simpleParamsDb, "Select * from ComplexTypes where row_id = 1");
    byte[] binaryData = <byte[]>getUntaintedData(value, "BLOB_TYPE");
    BinaryValue typeVal = new (binaryData);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE blob_type = ${typeVal}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBlobReadableByteChannelParam() {
    io:ReadableByteChannel byteChannel = getBlobColumnChannel();
    BlobValue typeVal = new (byteChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE blob_type = ${typeVal}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeClobStringParam() {
    ClobValue typeVal = new ("very long text");
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeClobReadableCharChannelParam() {
    io:ReadableCharacterChannel clobChannel = getClobColumnChannel();
    ClobValue typeVal = new (clobChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNClobReadableCharChannelParam() {
    io:ReadableCharacterChannel clobChannel = getClobColumnChannel();
    NClobValue typeVal = new (clobChannel);
    ParameterizedQuery sqlQuery = `SELECT * from ComplexTypes WHERE clob_type = ${typeVal}`;
    validateComplexTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateStringParam() {
    DateValue typeVal = new ("2017-02-03");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateString2Param() {
    DateValue typeVal = new ("2017-2-3");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateStringInvalidParam() {
    DateValue typeVal = new ("2017/2/3");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    record{}|error? result = trap queryMockClient(simpleParamsDb, sqlQuery);
    test:assertTrue(result is error);

    if (result is ApplicationError) {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from " +
                "DateTimeTypes WHERE date_type =  ? . java.lang.IllegalArgumentException"));
    } else {
        test:assertFail("ApplicationError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateTimeRecordParam() {
    time:Date date = {year: 2017, month:2, day: 3};
    DateValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

//@test:Config {
//    groups: ["query", "query-simple-params"]
//}
//function queryDateTimeRecordWithTimeZoneParam() {
//    time:Time date = checkpanic time:parse("2017-02-03T09:46:22.444-0500", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
//    DateValue typeVal = new (date);
//    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE date_type = ${typeVal}`;
//    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
//}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimeStringParam() {
    TimeValue typeVal = new ("11:35:45");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimeStringInvalidParam() {
    TimeValue typeVal = new ("11-35-45");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    record{}|error? result = trap queryMockClient(simpleParamsDb, sqlQuery);
    test:assertTrue(result is error);

    if (result is DatabaseError) {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from DateTimeTypes " +
        "WHERE time_type =  ? . data exception: invalid datetime format."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimeTimeRecordParam() {
    time:TimeOfDay date = {hour: 11, minute: 35, second:45};
    TimeValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

//@test:Config {
//    groups: ["query", "query-simple-params"]
//}
//function queryTimeTimeRecordWithTimeZoneParam() {
//    time:Time date = checkpanic time:parse("2017-02-03T11:35:45", "yyyy-MM-dd'T'HH:mm:ss");
//    TimeValue typeVal = new (date);
//    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE time_type = ${typeVal}`;
//    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
//}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampStringParam() {
    TimestampValue typeVal = new ("2017-02-03 11:53:00");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampStringInvalidParam() {
    TimestampValue typeVal = new ("2017/02/03 11:53:00");
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    record{}|error? result = trap queryMockClient(simpleParamsDb, sqlQuery);
    test:assertTrue(result is error);

    if (result is DatabaseError) {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: SELECT * from DateTimeTypes " +
        "WHERE timestamp_type =  ? . data exception: invalid datetime format."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampTimeRecordParam() {
    time:Utc date = checkpanic time:utcFromString("2017-02-03T11:53:00.00Z");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampTimeRecordWithTimeZoneParam() {
    time:Utc date = checkpanic time:utcFromString("2017-02-03T11:53:00.00Z");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDateTimeTimeRecordWithTimeZoneParam() {
    time:Civil date = {year: 2017, month:2, day: 3, hour: 11, minute: 53, second:0};
    DateTimeValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE datetime_type = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTimestampTimeRecordWithTimeZone2Param() {
    time:Utc date = checkpanic time:utcFromString("2008-08-08T20:08:08+08:00");
    TimestampValue typeVal = new (date);
    ParameterizedQuery sqlQuery = `SELECT * from DateTimeTypes WHERE timestamp_type2 = ${typeVal}`;
    validateDateTimeTypesTableResult(queryMockClient(simpleParamsDb, sqlQuery));
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryArrayBasicParams() {
    int[] dataint = [1, 2, 3];
    int[] datalong = [100000000, 200000000, 300000000];
    float[] datafloat = [245.23, 5559.49, 8796.123];
    float[] datadouble = [245.23, 5559.49, 8796.123];
    decimal[] datadecimal = [245, 5559, 8796];
    string[] datastring = ["Hello", "Ballerina"];
    boolean[] databoolean = [true, false, true];
    ArrayValue paraInt = new (dataint);
    ArrayValue paraLong = new (datalong);
    ArrayValue paraFloat = new (datafloat);
    ArrayValue paraDecimal = new (datadecimal);
    ArrayValue paraDouble = new (datadouble);
    ArrayValue paraString = new (datastring);
    ArrayValue paraBool = new (databoolean);

    ParameterizedQuery sqlQuery =
    `SELECT * from ArrayTypes WHERE int_array = ${paraInt}
                                AND long_array = ${paraLong}
                                AND float_array = ${paraFloat}
                                AND double_array = ${paraDouble}
                                AND decimal_array = ${paraDecimal}
                                AND string_array = ${paraString}
                                AND boolean_array = ${paraBool}`;
    record{}? returnData = queryMockClient(simpleParamsDb, sqlQuery);
    if (returnData is record{}) {
        test:assertEquals(returnData["INT_ARRAY"], [1, 2, 3]);
        test:assertEquals(returnData["LONG_ARRAY"], [100000000, 200000000, 300000000]);
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
function queryArrayBasicNullParams() {
    ParameterizedQuery sqlQuery =
        `SELECT * from ArrayTypes WHERE int_array is null AND long_array is null AND float_array
         is null AND double_array is null AND decimal_array is null AND string_array is null
         AND boolean_array is null`;

    record{}? returnData = queryMockClient(simpleParamsDb, sqlQuery);
    if (returnData is record{}) {
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

isolated function validateDataTableResult(record{}? returnData) {
    decimal decimalVal = 23.45;
    if (returnData is ()) {
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
    if (returnData is ()) {
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
    if (returnData is ()) {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 5);
        test:assertEquals(returnData["ROW_ID"], 1);
        test:assertEquals(returnData["CLOB_TYPE"], "very long text");
    }
}

isolated function validateDateTimeTypesTableResult(record{}? returnData) {
    if (returnData is ()) {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 7);
        test:assertEquals(returnData["ROW_ID"], 1);
        test:assertTrue(returnData["DATE_TYPE"].toString().startsWith("2017-02-03"));
    }
}

isolated function validateEnumTable(record{}? returnData) {
    if (returnData is ()) {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 2);
        test:assertEquals(returnData["ID"], 1);
        test:assertEquals(returnData["ENUM_TYPE"].toString(), "doctor");
    }
}

isolated function validateGeoTable(record{}? returnData) {
    if (returnData is ()) {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 2);
        test:assertEquals(returnData["ID"], 1);
        test:assertEquals(returnData["GEOM"].toString(), "POINT (7 52)");
    }
}

isolated function validateJsonTable(record{}? returnData) {
    if (returnData is ()) {
        test:assertFail("Returned data is nil");
    } else {
        test:assertEquals(returnData.length(), 2);
        test:assertEquals(returnData["ID"], 1);
        test:assertEquals(returnData["JSON_TYPE"], "{\"id\": 100, \"name\": \"Joe\", \"groups\": \"[2,5]\"}");
    }
}
