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
import ballerina/time;
import ballerina/jballerina.java;

string proceduresDb = "procedures";
string proceduresDB = urlPrefix + "9012/procedures";

type IntArray int[];
type StringArray string[];
type BooleanArray boolean[];
type FloatArray float[];
type DecimalArray decimal[];
type ByteArray byte[][];
type CivilArray time:Civil[];
type TimeOfDayArray time:TimeOfDay[];
type UtcArray time:Utc[];
type DateArray time:Date[];

type StringDataForCall record {
    string varchar_type;
    string charmax_type;
    string char_type;
    string charactermax_type;
    string character_type;
    string nvarcharmax_type;
};
 
type StringDataSingle record {
    string varchar_type;
};

@test:BeforeGroups {
	value: ["procedures"]	
} 
function initproceduresContainer() returns error? {
	check initializeDockerContainer("sql-procedures", "procedures", "9012", "procedures", "call-procedures-test-data.sql");
}

@test:AfterGroups {
	value: ["procedures"]	
} 
function cleanproceduresContainer() returns error? {
	check cleanDockerContainer("sql-procedures");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures1]
}
function testCallWithStringTypes() returns record {}|error? {
    int id = 2;
    MockClient dbClient = check new (url = proceduresDB, user = user, password = password);
    ProcedureCallResult ret = check dbClient->call(`call InsertStringData(${id},'test1', 'test2     ', 'c', 'test3', 'd', 'test4');`);
    ParameterizedQuery sqlQuery = `SELECT varchar_type,charmax_type, char_type, charactermax_type, character_type,
                   nvarcharmax_type from StringTypes where id = ${id};`;
    stream<StringDataForCall, Error?> queryData = dbClient->query(sqlQuery);
    StringDataForCall? returnData = ();
    error? e = queryData.forEach(function(StringDataForCall data) {
        returnData = data;
    });
    if(e is error){
        test:assertFail("Call procedure insert did not work properly");
    }
    else{
        StringDataForCall expectedDataRow = {
            varchar_type: "test1",
            charmax_type: "test2     ",
            char_type: "c",
            charactermax_type: "test3     ",
            character_type: "d",
            nvarcharmax_type: "test4"
        };
        test:assertEquals(returnData, expectedDataRow, "Call procedure insert and query did not match.");
    }
    check dbClient.close();
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithStringTypes]
}
function testCallWithStringTypesInParams() returns error? {
    int id = 3;
    MockClient dbClient = check new (url = proceduresDB, user = user, password = password);
    ProcedureCallResult ret = check dbClient->call(`call InsertStringData(${id},'test1', 'test2     ', 'c', 'test3', 'd', 'test4');`);
    ParameterizedQuery sqlQuery = `SELECT varchar_type,charmax_type, char_type, charactermax_type, character_type,
                   nvarcharmax_type from StringTypes where id = ${id};`;
    stream<StringDataForCall, Error?> queryData = dbClient->query(sqlQuery);
    StringDataForCall? returnData = ();
    error? e = queryData.forEach(function(StringDataForCall data) {
        returnData = data;
    });
    if(e is error){
        test:assertFail("Call procedure insert did not work properly");
    }
    else{
        StringDataForCall expectedDataRow = {
            varchar_type: "test1",
            charmax_type: "test2     ",
            char_type: "c",
            charactermax_type: "test3     ",
            character_type: "d",
            nvarcharmax_type: "test4"
        };
        test:assertEquals(returnData, expectedDataRow, "Call procedure insert and query did not match.");
    }
    check dbClient.close();
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithStringTypesInParams,testCreateProcedures2]
}
function testCallWithStringTypesOutParams() returns error? {
    IntegerValue paraID = new(1);
    VarcharOutParameter paraVarchar = new;
    CharOutParameter paraCharmax = new;
    CharOutParameter paraChar = new;
    CharOutParameter paraCharactermax = new;
    CharOutParameter paraCharacter = new;
    NVarcharOutParameter paraNvarcharmax = new;

    ParameterizedCallQuery callProcedureQuery = `call SelectStringDataWithOutParams(${paraID}, ${paraVarchar},
                            ${paraCharmax}, ${paraChar}, ${paraCharactermax}, ${paraCharacter}, ${paraNvarcharmax})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    test:assertEquals(paraVarchar.get(string), "test0", "2nd out parameter of procedure did not match.");
    test:assertEquals(paraCharmax.get(string), "test1     ", "3rd out parameter of procedure did not match.");
    test:assertEquals(paraChar.get(string), "a", "4th out parameter of procedure did not match.");
    test:assertEquals(paraCharactermax.get(string), "test2     ", "5th out parameter of procedure did not match.");
    test:assertEquals(paraCharacter.get(string), "b", "6th out parameter of procedure did not match.");
    test:assertEquals(paraNvarcharmax.get(string), "test3", "7th out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithStringTypesOutParams,testCreateProcedures3]
}
function testCallWithNumericTypesOutParams() returns error? {
    IntegerValue paraID = new(1);
    IntegerOutParameter paraInt = new;
    BigIntOutParameter paraBigInt = new;
    SmallIntOutParameter paraSmallInt = new;
    SmallIntOutParameter paraTinyInt = new;
    BitOutParameter paraBit = new;
    DecimalOutParameter paraDecimal = new;
    NumericOutParameter paraNumeric = new;
    FloatOutParameter paraFloat = new;
    RealOutParameter paraReal = new;
    DoubleOutParameter paraDouble = new;

    ParameterizedCallQuery callProcedureQuery = `call SelectNumericDataWithOutParams(${paraID}, ${paraInt},
                        ${paraBigInt}, ${paraSmallInt}, ${paraTinyInt}, ${paraBit}, ${paraDecimal}, ${paraNumeric},
                        ${paraFloat}, ${paraReal}, ${paraDouble})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    decimal paraDecimalVal= 1234.56;

    test:assertEquals(paraInt.get(int), 2147483647, "2nd out parameter of procedure did not match.");
    test:assertEquals(paraBigInt.get(int), 9223372036854774807, "3rd out parameter of procedure did not match.");
    test:assertEquals(paraSmallInt.get(int), 32767, "4th out parameter of procedure did not match.");
    test:assertEquals(paraTinyInt.get(int), 127, "5th out parameter of procedure did not match.");
    test:assertEquals(paraBit.get(boolean), true, "6th out parameter of procedure did not match.");
    test:assertEquals(paraDecimal.get(decimal), paraDecimalVal, "7th out parameter of procedure did not match.");
    test:assertEquals(paraNumeric.get(decimal), paraDecimalVal, "8th out parameter of procedure did not match.");
    test:assertTrue((check paraFloat.get(float)) > 1234.0, "9th out parameter of procedure did not match.");
    test:assertTrue((check paraReal.get(float)) > 1234.0, "10th out parameter of procedure did not match.");
    test:assertEquals(paraDouble.get(float), 1234.56, "11th out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithStringTypesOutParams,testCreateProcedures3]
}
function testCallWithNumericTypesOutParamsForInvalidInValue() returns error? {
    IntegerValue paraID = new(2);
    IntegerOutParameter paraInt = new;
    BigIntOutParameter paraBigInt = new;
    SmallIntOutParameter paraSmallInt = new;
    SmallIntOutParameter paraTinyInt = new;
    BitOutParameter paraBit = new;
    DecimalOutParameter paraDecimal = new;
    NumericOutParameter paraNumeric = new;
    FloatOutParameter paraFloat = new;
    RealOutParameter paraReal = new;
    DoubleOutParameter paraDouble = new;

    ParameterizedCallQuery callProcedureQuery = `call SelectNumericDataWithOutParams(${paraID}, ${paraInt},
                        ${paraBigInt}, ${paraSmallInt}, ${paraTinyInt}, ${paraBit}, ${paraDecimal}, ${paraNumeric},
                        ${paraFloat}, ${paraReal}, ${paraDouble})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    test:assertEquals(paraInt.get(int), 0, "2nd out parameter of procedure did not match.");
    test:assertEquals(paraBigInt.get(int), 0, "3rd out parameter of procedure did not match.");
    test:assertEquals(paraSmallInt.get(int), 0, "4th out parameter of procedure did not match.");
    test:assertEquals(paraTinyInt.get(int), 0, "5th out parameter of procedure did not match.");
    test:assertEquals(paraBit.get(boolean), false, "6th out parameter of procedure did not match.");
    test:assertEquals(paraDecimal.get(decimal), (), "7th out parameter of procedure did not match.");
    test:assertEquals(paraNumeric.get(decimal), (), "8th out parameter of procedure did not match.");
    test:assertTrue((check paraFloat.get(float)) >= 0.0, "9th out parameter of procedure did not match.");
    test:assertTrue((check paraReal.get(float)) >= 0.0, "10th out parameter of procedure did not match.");
    test:assertEquals(paraDouble.get(float), 0.0, "11th out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithNumericTypesOutParams,testCreateProcedures4]
}
function testCallWithStringTypesInoutParams() returns error? {
    IntegerValue paraID = new(1);
    InOutParameter paraVarchar = new("test varchar");
    InOutParameter paraCharmax = new("test char");
    InOutParameter paraChar = new("T");
    InOutParameter paraCharactermax = new("test c_max");
    InOutParameter paraCharacter = new("C");
    InOutParameter paraNvarcharmax = new("test_nchar");

    ParameterizedCallQuery callProcedureQuery = `call SelectStringDataWithInoutParams(${paraID}, ${paraVarchar},
                             ${paraCharmax}, ${paraChar}, ${paraCharactermax}, ${paraCharacter}, ${paraNvarcharmax})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    test:assertEquals(paraVarchar.get(string), "test0", "2nd out parameter of procedure did not match.");
    test:assertEquals(paraCharmax.get(string), "test1     ", "3rd out parameter of procedure did not match.");
    test:assertEquals(paraChar.get(string), "a", "4th out parameter of procedure did not match.");
    test:assertEquals(paraCharactermax.get(string), "test2     ", "5th out parameter of procedure did not match.");
    test:assertEquals(paraCharacter.get(string), "b", "6th out parameter of procedure did not match.");
    test:assertEquals(paraNvarcharmax.get(string), "test3", "7th out parameter of procedure did not match.");
}


@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithStringTypesInoutParams,testCreateProcedures5]
}
function testCallWithNumericTypesInoutParams() returns error? {
    decimal paraInDecimalVal= -1234.56;

    IntegerValue paraID = new(1);
    InOutParameter paraInt = new(-2147483647);
    InOutParameter paraBigInt = new(-9223372036854774807);
    InOutParameter paraSmallInt = new(-32767);
    InOutParameter paraTinyInt = new(-127);
    InOutParameter paraBit = new(false);
    InOutParameter paraDecimal = new(paraInDecimalVal);
    InOutParameter paraNumeric = new(paraInDecimalVal);
    InOutParameter paraFloat = new(-1234.56);
    InOutParameter paraReal = new(-1234.56);
    InOutParameter paraDouble = new(-1234.56);

    ParameterizedCallQuery callProcedureQuery = `call SelectNumericDataWithInoutParams(${paraID}, ${paraInt}, ${paraBigInt},
                                ${paraSmallInt}, ${paraTinyInt}, ${paraBit}, ${paraDecimal}, ${paraNumeric},
                                 ${paraFloat}, ${paraReal}, ${paraDouble})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    decimal paraDecimalVal= 1234.56;

    test:assertEquals(paraInt.get(int), 2147483647, "2nd out parameter of procedure did not match.");
    test:assertEquals(paraBigInt.get(int), 9223372036854774807, "3rd out parameter of procedure did not match.");
    test:assertEquals(paraSmallInt.get(int), 32767, "4th out parameter of procedure did not match.");
    test:assertEquals(paraTinyInt.get(int), 127, "5th out parameter of procedure did not match.");
    test:assertEquals(paraBit.get(boolean), true, "6th out parameter of procedure did not match.");
    test:assertEquals(paraDecimal.get(decimal), paraDecimalVal, "7th out parameter of procedure did not match.");
    test:assertEquals(paraNumeric.get(decimal), paraDecimalVal, "8th out parameter of procedure did not match.");
    test:assertTrue((check paraFloat.get(float)) > 1234.0, "9th out parameter of procedure did not match.");
    test:assertTrue((check paraReal.get(float)) > 1234.0, "10th out parameter of procedure did not match.");
    test:assertEquals(paraDouble.get(float), 1234.56, "11th out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithStringTypesInoutParams, testCreateProcedures9]
}
function testCallWithAllTypesInoutParamsAsObjectValues() returns error? {
    IntegerValue paraID = new(1);
    VarcharValue varCharVal = new();
    CharValue charVal = new();
    NVarcharValue nVarCharVal = new();
    BitValue bitVal = new();
    BooleanValue booleanVal = new();
    IntegerValue intVal = new();
    SmallIntValue smallIntVal = new();
    BigIntValue bigIntVal = new();
    NumericValue numericVal = new();
    DoubleValue doubleVal = new();
    RealValue realVal = new();
    FloatValue floatVal = new();
    DecimalValue decimalVal = new();
    VarBinaryValue varBinaryVal = new();
    BinaryValue binaryVal = new();
    ClobValue clobVal = new();
    TimeValue timeVal = new();
    DateValue dateVal = new();
    TimestampValue timestampVal = new();
    DateTimeValue datetimeVal = new();
    int[] intArrayVal = [1, 2];
    string[] strArrayVal = ["Hello", "Ballerina"];
    boolean[] booArrayVal = [true, false, true];
    float[] floArrayVal = [245.23, 5559.49, 8796.123];
    decimal[] decArrayVal = [245, 5559, 8796];
    byte[][] byteArrayVal = [<byte[]>[32], <byte[]>[96], <byte[]>[128]];
    string[] emptyArrayVal = [];

    InOutParameter paraVarChar = new(varCharVal);
    InOutParameter paraChar = new(charVal);
    InOutParameter paraNvarchar = new(nVarCharVal);
    InOutParameter paraBit = new(bitVal);
    InOutParameter paraBoolean = new(booleanVal);
    InOutParameter paraInt = new(intVal);
    InOutParameter paraBigInt = new(bigIntVal);
    InOutParameter paraSmallInt = new(smallIntVal);
    InOutParameter paraNumeric = new(numericVal);
    InOutParameter paraFloat = new(floatVal);
    InOutParameter paraReal = new(realVal);
    InOutParameter paraDouble = new(doubleVal);
    InOutParameter paraDecimal = new(decimalVal);
    InOutParameter paraVarBinary = new (varBinaryVal);
    InOutParameter paraBinary = new (binaryVal);
    InOutParameter paraClob = new(clobVal);
    InOutParameter paraDateTime = new(datetimeVal);
    InOutParameter paraDate = new(dateVal);
    InOutParameter paraTime = new(timeVal);
    InOutParameter paraTimestamp = new(timestampVal);
    InOutParameter paraIntArray = new(intArrayVal);
    InOutParameter paraStrArray = new(strArrayVal);
    InOutParameter paraFloArray = new(floArrayVal);
    InOutParameter paraDecArray = new(decArrayVal);
    InOutParameter paraBooArray = new(booArrayVal);
    InOutParameter paraByteArray = new(byteArrayVal);
    InOutParameter paraEmptyArray = new(emptyArrayVal);

    ParameterizedCallQuery callProcedureQuery = `call SelectOtherDataWithInoutParams(${paraID}, ${paraVarChar}, ${paraChar},
        ${paraNvarchar}, ${paraBit}, ${paraBoolean}, ${paraInt}, ${paraBigInt}, ${paraSmallInt}, ${paraNumeric}, ${paraFloat},
        ${paraReal}, ${paraDouble}, ${paraDecimal}, ${paraVarBinary}, ${paraBinary}, ${paraClob}, ${paraDateTime}, ${paraDate}, ${paraTime},
        ${paraTimestamp}, ${paraIntArray}, ${paraStrArray}, ${paraFloArray}, ${paraDecArray}, ${paraBooArray}, ${paraByteArray},
        ${paraEmptyArray})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    string clobType = "very long text";
    var varBinaryType = "77736f322062616c6c6572696e612062696e61727920746573742e".toBytes();
    time:Civil dateTimeRecord = {year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55};

    test:assertEquals(paraClob.get(string), clobType, "Clob out parameter of procedure did not match.");
    test:assertEquals(paraVarBinary.get(byte), varBinaryType, "VarBinary out parameter of procedure did not match.");
    test:assertEquals(paraDateTime.get(time:Civil), dateTimeRecord, "DateTime out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures8]
}
function testCallWithInoutParams() returns error? {

    IntegerValue paraID = new(1);
    VarcharValue varCharVal = new();
    CharValue charVal = new();
    NVarcharValue nVarCharVal = new();
    BitValue bitVal = new();
    BooleanValue booleanVal = new();
    IntegerValue intVal = new();
    SmallIntValue smallIntVal = new();
    BigIntValue bigIntVal = new();
    NumericValue numericVal = new();
    DoubleValue doubleVal = new();
    RealValue realVal = new();
    FloatValue floatVal = new();
    DecimalValue decimalVal = new();
    VarBinaryValue varBinaryVal = new();
    BinaryValue binaryVal = new();
    ClobValue clobVal = new();
    TimeValue timeVal = new();
    DateValue dateVal = new();
    TimestampValue timestampVal = new();
    DateTimeValue datetimeVal = new();
    int[] intArr = [1, 2];
    ArrayValue intArrayVal = new(intArr);
    string[] strArr = ["Hello", "Ballerina"];
    ArrayValue strArrayVal = new(strArr);
    boolean[] booArr = [false, false, true];
    ArrayValue booArrayVal = new(booArr);
    float[] floArr = [245.23, 5559.49, 8796.123];
    ArrayValue floArrayVal = new(floArr);
    decimal[] decArr = [245.34, 5559.43, 8796.43];
    ArrayValue decArrayVal = new(decArr);
    byte[][] byteArr = [<byte[]>[32], <byte[]>[96], <byte[]>[128]];
    ArrayValue byteArrayVal = new(byteArr);
    ArrayValue emptyArrayVal = new();

    InOutParameter paraVarChar = new(varCharVal);
    InOutParameter paraChar = new(charVal);
    InOutParameter paraNvarchar = new(nVarCharVal);
    InOutParameter paraBit = new(bitVal);
    InOutParameter paraBoolean = new(booleanVal);
    InOutParameter paraInt = new(intVal);
    InOutParameter paraBigInt = new(bigIntVal);
    InOutParameter paraSmallInt = new(smallIntVal);
    InOutParameter paraNumeric = new(numericVal);
    InOutParameter paraFloat = new(floatVal);
    InOutParameter paraReal = new(realVal);
    InOutParameter paraDouble = new(doubleVal);
    InOutParameter paraDecimal = new(decimalVal);
    InOutParameter paraVarBinary = new (varBinaryVal);
    InOutParameter paraBinary = new (binaryVal);
    InOutParameter paraClob = new(clobVal);
    InOutParameter paraDateTime = new(datetimeVal);
    InOutParameter paraDate = new(dateVal);
    InOutParameter paraTime = new(timeVal);
    InOutParameter paraTimestamp = new(timestampVal);
    InOutParameter paraIntArray = new(intArrayVal);
    InOutParameter paraStrArray = new(strArrayVal);
    InOutParameter paraFloArray = new(floArrayVal);
    InOutParameter paraDecArray = new(decArrayVal);
    InOutParameter paraBooArray = new(booArrayVal);
    InOutParameter paraByteArray = new(byteArrayVal);
    InOutParameter paraEmptyArray = new(emptyArrayVal);

    ParameterizedCallQuery callProcedureQuery = `call SelectOtherDataWithInoutParams(${paraID}, ${paraVarChar}, ${paraChar},
        ${paraNvarchar}, ${paraBit}, ${paraBoolean}, ${paraInt}, ${paraBigInt}, ${paraSmallInt}, ${paraNumeric}, ${paraFloat},
        ${paraReal}, ${paraDouble}, ${paraDecimal}, ${paraVarBinary}, ${paraBinary}, ${paraClob}, ${paraDateTime}, ${paraDate}, ${paraTime},
        ${paraTimestamp}, ${paraIntArray}, ${paraStrArray}, ${paraFloArray}, ${paraDecArray}, ${paraBooArray}, ${paraByteArray},
        ${paraEmptyArray})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();
    decimal[] decimalArray = [245,5559,8796];
    byte[][] byteArray = [[119,115,111,50,32,98,97,108,108,101,114,105,110,97,32,98,108,111,98,32,116,101,115,116,46]];
    test:assertEquals(paraIntArray.get(IntArray), [1,2,3], "Int arra out parameter of procedure did not match.");
    test:assertEquals(paraStrArray.get(StringArray), ["Hello","Ballerina"], "String array out parameter " +
    "of procedure did not match.");
    test:assertEquals(paraFloArray.get(FloatArray), [245.23,5559.49,8796.123], "Float array out parameter of " +
    "procedure did not match.");
    test:assertEquals(paraDecArray.get(DecimalArray), decimalArray , "Decimal array out parameter " +
    "of procedure did not match.");
    test:assertEquals(paraBooArray.get(BooleanArray), [true,false,true], "Boolean array out parameter " +
    "of procedure did not match.");
    test:assertEquals(paraByteArray.get(ByteArray), byteArray, "Byte array out parameter of " +
    "procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithStringTypesInoutParams,testCreateProcedures5]
}
function testErroneousCallWithNumericTypesInoutParams() returns error? {
    IntegerValue paraID = new(1);

    ParameterizedCallQuery callProcedureQuery = `call SelectNumericDataWithInoutParams(${paraID})`;
    ProcedureCallResult|error ret = getProcedureCallResultFromMockClient(callProcedureQuery);
    test:assertTrue(ret is error);

    if ret is DatabaseError {
        test:assertTrue(ret.message().startsWith("Error while executing SQL query: call " +
        "SelectNumericDataWithInoutParams( ? ). user lacks privilege or object not found in statement " +
        "[call SelectNumericDataWithInoutParams( ? )]."));
    } else {
        test:assertFail("DatabaseError Error expected.");
    }
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures6]
}
function testCallWithDateTimeTypesWithOutParams() returns error? {
    IntegerValue paraID = new(1);
    DateOutParameter paraDate = new;
    TimeOutParameter paraTime = new;
    DateTimeOutParameter paraDateTime = new;
    TimeWithTimezoneOutParameter paraTimeWithTz = new;
    TimestampOutParameter paraTimestamp = new;
    TimestampWithTimezoneOutParameter paraTimestampWithTz = new;

    ParameterizedCallQuery callProcedureQuery = `call SelectDateTimeDataWithOutParams(${paraID}, ${paraDate},
                                    ${paraTime}, ${paraDateTime}, ${paraTimeWithTz}, ${paraTimestamp}, ${paraTimestampWithTz})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();
    test:assertEquals(paraDate.get(string), "2017-05-23", "Date out parameter of procedure did not match.");
    test:assertEquals(paraTime.get(string), "14:15:23", "Time out parameter of procedure did not match.");
    test:assertEquals(paraTimeWithTz.get(string), "16:33:55+06:30", "Time out parameter of procedure did not match.");
    test:assertEquals(paraTimestamp.get(string), "2017-01-25 16:33:55.0", "Timestamp out parameter of procedure did not match.");
    test:assertEquals(paraTimestampWithTz.get(string), "2017-01-25T16:33:55-08:00", "Date Time out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures6]
}
function testCallWithDateTimeTypeRecordsWithOutParams() returns error? {
    IntegerValue paraID = new(1);
    DateOutParameter paraDate = new;
    TimeOutParameter paraTime = new;
    DateTimeOutParameter paraDateTime = new;
    TimeWithTimezoneOutParameter paraTimeWithTz = new;
    TimestampOutParameter paraTimestamp = new;
    TimestampWithTimezoneOutParameter paraTimestampWithTz = new;

    ParameterizedCallQuery callProcedureQuery = `call SelectDateTimeDataWithOutParams(${paraID}, ${paraDate},
                                    ${paraTime}, ${paraDateTime}, ${paraTimeWithTz}, ${paraTimestamp}, ${paraTimestampWithTz})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    time:Date dateRecord = {year: 2017, month: 5, day: 23};
    time:TimeOfDay timeRecord = {hour: 14, minute: 15, second:23};
    time:Civil timestampRecord = {year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55};
    time:TimeOfDay timeWithTzRecord = {utcOffset: {hours: 6, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+06:30"};
    time:Civil timestampWithTzRecord = {utcOffset: {hours: -8, minutes: 0}, timeAbbrev: "-08:00", year:2017,
                                        month:1, day:25, hour: 16, minute: 33, second:55};

    test:assertEquals(paraDate.get(time:Date), dateRecord, "Date out parameter of procedure did not match.");
    test:assertEquals(paraTime.get(time:TimeOfDay), timeRecord, "Time out parameter of procedure did not match.");
    test:assertEquals(paraDateTime.get(time:Civil), timestampRecord, "DateTime out parameter of procedure did not match.");
    test:assertEquals(paraTimeWithTz.get(time:TimeOfDay), timeWithTzRecord, "Time with Timezone out parameter of procedure did not match.");
    test:assertEquals(paraTimestamp.get(time:Civil), timestampRecord, "Timestamp out parameter of procedure did not match.");
    test:assertEquals(paraTimestampWithTz.get(time:Civil), timestampWithTzRecord, "Timestamp with Timezone out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures7]
}
function testCallWithTimestamptzRetrievalWithOutParams() returns error? {
    IntegerValue paraID = new(1);
    TimestampWithTimezoneOutParameter paraTimestampWithTz = new;

    ParameterizedCallQuery callProcedureQuery = `call SelectTimestamptzWithOutParams(${paraID}, ${paraTimestampWithTz})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    string timestampWithTzRecordString = "2017-01-25T16:33:55-08:00";
    time:Civil timestampWithTzRecordCivil = {utcOffset: {hours: -8, minutes: 0}, timeAbbrev: "-08:00", year:2017,
                                        month:1, day:25, hour: 16, minute: 33, second:55};
    time:Utc timestampWithTzRecordUtc = check time:utcFromCivil(timestampWithTzRecordCivil);

    test:assertEquals(paraTimestampWithTz.get(string), timestampWithTzRecordString, "Timestamp with Timezone out parameter of procedure did not match.");
    test:assertEquals(paraTimestampWithTz.get(time:Civil), timestampWithTzRecordCivil, "Timestamp with Timezone out parameter of procedure did not match.");
    test:assertEquals(paraTimestampWithTz.get(time:Utc), timestampWithTzRecordUtc, "Timestamp with Timezone out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures8]
}
function testCallWithOtherDataTypesWithOutParams() returns error? {
    IntegerValue paraID = new(1);
    BlobOutParameter paraBlob = new;
    ClobOutParameter paraClob = new;
    VarBinaryOutParameter paraVarBinary = new;
    ArrayOutParameter paraIntArray = new;
    ArrayOutParameter paraStringArray = new;
    BooleanOutParameter paraBoolean = new;
    BinaryOutParameter paraBinary = new;
    TextOutParameter paraText = new;

    ParameterizedCallQuery callProcedureQuery = `call SelectOtherDataTypesWithOutParams(${paraID}, ${paraClob},
                                    ${paraVarBinary}, ${paraIntArray}, ${paraStringArray}, ${paraBinary}, ${paraBoolean})`;

    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    string clobType = "very long text";
    var varBinaryType = "77736f322062616c6c6572696e612062696e61727920746573742e".toBytes();
    int[] int_array = [1, 2, 3];
    string[] string_array = ["Hello", "Ballerina"];
    var binaryType = "77736f322062616c6c6572696e612062696e61727920746573742e".toBytes();

    test:assertEquals(paraClob.get(string), clobType, "Clob out parameter of procedure did not match.");
    test:assertEquals(paraVarBinary.get(byte), varBinaryType, "VarBinary out parameter of procedure did not match.");
    test:assertEquals(paraBinary.get(byte), binaryType, "Binary out parameter of procedure did not match.");
    test:assertEquals(paraBoolean.get(boolean), true, "Boolean out parameter of procedure did not match.");
    test:assertEquals(paraIntArray.get(IntArray), int_array, "Int array out parameter of procedure did not match.");
    test:assertEquals(paraStringArray.get(StringArray), string_array, "String array out parameter of procedure did not match.");
}

distinct class RandomOutParameter {
    *OutParameter;
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures8]
}
function testCallWithOtherDataTypesWithInvalidOutParams() returns error? {
    IntegerValue paraID = new(1);
    BlobOutParameter paraBlob = new;
    ClobOutParameter paraClob = new;
    VarBinaryOutParameter paraVarBinary = new;
    ArrayOutParameter paraIntArray = new;
    RandomOutParameter paraStringArray = new;
    BooleanOutParameter paraBoolean = new;
    BinaryOutParameter paraBinary = new;
    TextOutParameter paraText = new;
    NCharOutParameter paraNChar = new;
    NClobOutParameter paraNClob = new;
    RowOutParameter paraRowOut = new;
    RefOutParameter paraRefOut = new;
    StructOutParameter paraStruct = new;
    XMLOutParameter paraXml = new;

    ParameterizedCallQuery callProcedureQuery = `call SelectOtherDataTypesWithOutParams(${paraID}, ${paraClob},
                                    ${paraVarBinary} , ${paraIntArray}, ${paraStringArray}, ${paraBinary}, ${paraBoolean})`;

    ProcedureCallResult|error ret = getProcedureCallResultFromMockClient(callProcedureQuery);
    test:assertTrue(ret is error);
}

@test:Config {
    groups: ["procedures"]
}
function testCreateProcedures1() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE InsertStringData(IN p_id INTEGER,
                                  IN p_varchar_type VARCHAR(255),
                                  IN p_charmax_type CHAR(10),
                                  IN p_char_type CHAR,
                                  IN p_charactermax_type CHARACTER(10),
                                  IN p_character_type CHARACTER,
                                  IN p_nvarcharmax_type NVARCHAR(255))
       MODIFIES SQL DATA
              INSERT INTO StringTypes(id, varchar_type, charmax_type, char_type, charactermax_type, character_type, nvarcharmax_type)
              VALUES (p_id, p_varchar_type, p_charmax_type, p_char_type, p_charactermax_type, p_character_type, p_nvarcharmax_type);
    `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures1]
}
function testCreateProcedures2() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectStringDataWithOutParams (IN p_id INT, OUT p_varchar_type VARCHAR(255),
                                                OUT p_charmax_type CHAR(10), OUT p_char_type CHAR, OUT p_charactermax_type CHARACTER(10),
                                                OUT p_character_type CHARACTER, OUT p_nvarcharmax_type NVARCHAR(255))
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT varchar_type INTO p_varchar_type FROM StringTypes where id = p_id;
                SELECT charmax_type INTO p_charmax_type FROM StringTypes where id = p_id;
                SELECT char_type INTO p_char_type FROM StringTypes where id = p_id;
                SELECT charactermax_type INTO p_charactermax_type FROM StringTypes where id = p_id;
                SELECT character_type INTO p_character_type FROM StringTypes where id = p_id;
                SELECT nvarcharmax_type INTO p_nvarcharmax_type FROM StringTypes where id = p_id;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures2]
}
function testCreateProcedures3() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectNumericDataWithOutParams (IN p_id INT, OUT p_int_type INT,OUT p_bigint_type BIGINT,
                                                 OUT p_smallint_type SMALLINT, OUT p_tinyint_type TINYINT,OUT p_bit_type BIT, OUT p_decimal_type DECIMAL(10,2),
                                                 OUT p_numeric_type NUMERIC(10,2), OUT p_float_type FLOAT, OUT p_real_type REAL, OUT p_double_type DOUBLE)
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT int_type INTO p_int_type FROM NumericTypes where id = p_id;
                SELECT bigint_type INTO p_bigint_type FROM NumericTypes where id = p_id;
                SELECT smallint_type INTO p_smallint_type FROM NumericTypes where id = p_id;
                SELECT tinyint_type INTO p_tinyint_type FROM NumericTypes where id = p_id;
                SELECT bit_type INTO p_bit_type FROM NumericTypes where id = p_id;
                SELECT decimal_type INTO p_decimal_type FROM NumericTypes where id = p_id;
                SELECT numeric_type INTO p_numeric_type FROM NumericTypes where id = p_id;
                SELECT float_type INTO p_float_type FROM NumericTypes where id = p_id;
                SELECT real_type INTO p_real_type FROM NumericTypes where id = p_id;
                SELECT double_type INTO p_double_type FROM NumericTypes where id = p_id;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures3]
}
function testCreateProcedures4() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectStringDataWithInoutParams (IN p_id INT, INOUT p_varchar_type VARCHAR(255),
                                                INOUT p_charmax_type CHAR(10), INOUT p_char_type CHAR, INOUT p_charactermax_type CHARACTER(10),
                                                INOUT p_character_type CHARACTER, INOUT p_nvarcharmax_type NVARCHAR(255))
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT varchar_type INTO p_varchar_type FROM StringTypes where id = p_id;
                SELECT charmax_type INTO p_charmax_type FROM StringTypes where id = p_id;
                SELECT char_type INTO p_char_type FROM StringTypes where id = p_id;
                SELECT charactermax_type INTO p_charactermax_type FROM StringTypes where id = p_id;
                SELECT character_type INTO p_character_type FROM StringTypes where id = p_id;
                SELECT nvarcharmax_type INTO p_nvarcharmax_type FROM StringTypes where id = p_id;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures4]
}
function testCreateProcedures5() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectNumericDataWithInoutParams (IN p_id INT, INOUT p_int_type INT,INOUT p_bigint_type BIGINT,
                                                 INOUT p_smallint_type SMALLINT, INOUT p_tinyint_type TINYINT,INOUT p_bit_type BIT, INOUT p_decimal_type DECIMAL(10,2),
                                                 INOUT p_numeric_type NUMERIC(10,2), INOUT p_float_type FLOAT, INOUT p_real_type REAL, INOUT p_double_type DOUBLE)
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT int_type INTO p_int_type FROM NumericTypes where id = p_id;
                SELECT bigint_type INTO p_bigint_type FROM NumericTypes where id = p_id;
                SELECT smallint_type INTO p_smallint_type FROM NumericTypes where id = p_id;
                SELECT tinyint_type INTO p_tinyint_type FROM NumericTypes where id = p_id;
                SELECT bit_type INTO p_bit_type FROM NumericTypes where id = p_id;
                SELECT decimal_type INTO p_decimal_type FROM NumericTypes where id = p_id;
                SELECT numeric_type INTO p_numeric_type FROM NumericTypes where id = p_id;
                SELECT float_type INTO p_float_type FROM NumericTypes where id = p_id;
                SELECT real_type INTO p_real_type FROM NumericTypes where id = p_id;
                SELECT double_type INTO p_double_type FROM NumericTypes where id = p_id;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures5]
}
function testCreateProcedures6() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectDateTimeDataWithOutParams (IN p_id INT, OUT p_date_type DATE, OUT p_time_type TIME, OUT p_datetime_type DATETIME,
                                                OUT p_timewithtz_type TIME WITH TIME ZONE,  OUT p_timestamp_type TIMESTAMP,
                                                OUT p_timestampwithtz_type TIMESTAMP WITH TIME ZONE)
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT date_type INTO p_date_type FROM DateTimeTypes where id = p_id;
                SELECT time_type INTO p_time_type FROM DateTimeTypes where id = p_id;
                SELECT datetime_type INTO p_datetime_type FROM DateTimeTypes where id = p_id;
                SELECT timewithtz_type INTO p_timewithtz_type FROM DateTimeTypes where id = p_id;
                SELECT timestamp_type INTO p_timestamp_type FROM DateTimeTypes where id = p_id;
                SELECT timestampwithtz_type INTO p_timestampwithtz_type FROM DateTimeTypes where id = p_id;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures6]
}
function testCreateProcedures7() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectTimestamptzWithOutParams (IN p_id INT, OUT p_timestampwithtz_type TIMESTAMP WITH TIME ZONE)
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT timestampwithtz_type INTO p_timestampwithtz_type FROM DateTimeTypes where id = p_id;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}


@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures7]
}
function testCreateProcedures8() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectOtherDataTypesWithOutParams (IN p_id INT, OUT p_clob_type CLOB,
                                                OUT p_var_binary_type VARBINARY(27), OUT p_int_array_type INT ARRAY,
                                                OUT p_string_array_type VARCHAR(50) ARRAY, OUT p_binary_type BINARY(27),
                                                OUT p_boolean_type BOOLEAN)
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT clob_type INTO p_clob_type FROM OtherTypes where id = p_id;
                SELECT var_binary_type INTO p_var_binary_type FROM OtherTypes where id = p_id;
                SELECT binary_type INTO p_binary_type FROM OtherTypes where id = p_id;
                SELECT boolean_type INTO p_boolean_type FROM OtherTypes where id = p_id;
                SELECT int_array_type INTO p_int_array_type FROM OtherTypes where id = p_id;
                SELECT string_array_type INTO p_string_array_type FROM OtherTypes where id = p_id;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}

@test:Config {
    groups: ["procedures"]
}
function testCreateProcedures9() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectOtherDataWithInoutParams (IN p_id INT, INOUT p_varchar_type VARCHAR(255), INOUT p_char_type CHAR,
                INOUT p_nvarcharmax_type NVARCHAR(255), INOUT p_bit_type BIT, INOUT p_boolean_type BOOLEAN, INOUT p_int_type INT,
                INOUT p_bigint_type BIGINT, INOUT p_smallint_type SMALLINT, INOUT p_numeric_type NUMERIC(10,2),
                INOUT p_float_type FLOAT, INOUT p_real_type REAL, INOUT p_double_type DOUBLE, INOUT p_decimal_type DECIMAL(10,2),
                INOUT p_var_binary_type VARBINARY(27),INOUT p_binary_type BINARY(27), INOUT p_clob_type CLOB, INOUT p_datetime_type DATETIME,
                INOUT p_date_type DATE, INOUT p_time_type TIME, INOUT p_timestamp_type TIMESTAMP, INOUT p_int_array_type INT ARRAY,
                INOUT p_string_array_type VARCHAR(50) ARRAY, INOUT p_float_array_type FLOAT ARRAY, INOUT p_decimal_array_type DECIMAL ARRAY,
                INOUT p_boolean_array_type BOOLEAN ARRAY, INOUT p_byte_array_type VARBINARY(27) ARRAY, INOUT p_empty_array_type VARCHAR(50) ARRAY)
            READS SQL DATA DYNAMIC RESULT SETS 1
            BEGIN ATOMIC
                SELECT varchar_type INTO p_varchar_type FROM StringTypes where id = p_id;
                SELECT char_type INTO p_char_type FROM StringTypes where id = p_id;
                SELECT nvarcharmax_type INTO p_nvarcharmax_type FROM StringTypes where id = p_id;
                SELECT bit_type INTO p_bit_type FROM NumericTypes where id = p_id;
                SELECT boolean_type INTO p_boolean_type FROM OtherTypes where id = p_id;
                SELECT int_type INTO p_int_type FROM NumericTypes where id = p_id;
                SELECT bigint_type INTO p_bigint_type FROM NumericTypes where id = p_id;
                SELECT smallint_type INTO p_smallint_type FROM NumericTypes where id = p_id;
                SELECT decimal_type INTO p_decimal_type FROM NumericTypes where id = p_id;
                SELECT numeric_type INTO p_numeric_type FROM NumericTypes where id = p_id;
                SELECT float_type INTO p_float_type FROM NumericTypes where id = p_id;
                SELECT real_type INTO p_real_type FROM NumericTypes where id = p_id;
                SELECT double_type INTO p_double_type FROM NumericTypes where id = p_id;
                SELECT var_binary_type INTO p_var_binary_type FROM OtherTypes where id = p_id;
                SELECT binary_type INTO p_binary_type FROM OtherTypes where id = p_id;
                SELECT clob_type INTO p_clob_type FROM OtherTypes where id = p_id;
                SELECT datetime_type INTO p_datetime_type FROM DateTimeTypes where id = p_id;
                SELECT date_type INTO p_date_type FROM DateTimeTypes where id = p_id;
                SELECT time_type INTO p_time_type FROM DateTimeTypes where id = p_id;
                SELECT timestamp_type INTO p_timestamp_type FROM DateTimeTypes where id = p_id;
                SELECT int_array_type INTO p_int_array_type FROM OtherTypes where id = p_id;
                SELECT string_array_type INTO p_string_array_type FROM OtherTypes where id = p_id;
                SELECT boolean_array INTO p_boolean_array_type FROM ArrayTypes where row_id = p_id;
                SELECT float_array INTO p_float_array_type FROM ArrayTypes where row_id = p_id;
                SELECT decimal_array INTO p_decimal_array_type FROM ArrayTypes where row_id = p_id;
                SELECT blob_array INTO p_byte_array_type FROM ArrayTypes where row_id = p_id;
                SELECT string_array_type INTO p_empty_array_type FROM OtherTypes where id = p_id;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());
}

type Person record {
    int id;
    string name;
    int age;
    string birthday;
    string country_code;
};

@test:Config {
    groups: ["procedures"]
}
function testMultipleRecords() returns error? {
    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE FetchMultipleRecords ()
            READS SQL DATA DYNAMIC RESULT SETS 1
            BEGIN ATOMIC
                 declare curs cursor with return for select * from MultipleRecords;
                 open curs;
            END
        `;

    _ = check createSqlProcedure(createProcedure);

    ParameterizedCallQuery callProcedureQuery = `call FetchMultipleRecords()`;

    MockClient dbClient = check new (url = proceduresDB, user = user, password = password);
    ProcedureCallResult result = check dbClient->call(callProcedureQuery, [Person]);
    boolean|Error status = result.getNextQueryResult();
    stream<record {}, Error?>? streamData = result.queryResult;
    check result.close();
    check dbClient.close();
    test:assertTrue(streamData is stream<record {}, Error?>, "streamData is nil.");
    test:assertTrue(status is boolean, "streamData is not boolean.");
}

@test:Config {
    groups: ["procedures"]
}
function testCallWithAllArrayTypesInoutParamsAsObjectValues() returns error? {
    float float1 = 19.21;
    float float2 = 492.98;
    SmallIntArrayValue paraSmallint = new([1211, 478]);
    IntegerArrayValue paraInt = new([121, 498]);
    BigIntArrayValue paraLong = new ([121, 498]);
    float[] paraFloat = [19.21, 492.98];
    DoubleArrayValue paraDouble = new ([float1, float2]);
    RealArrayValue paraReal = new ([float1, float2]);
    DecimalArrayValue paraDecimal = new ([<decimal> 12.245, <decimal> 13.245]);
    NumericArrayValue paraNumeric = new ([float1, float2]);
    CharArrayValue paraChar = new (["Char value", "Character"]);
    VarcharArrayValue paraVarchar = new (["Varchar value", "Varying Char"]);
    NVarcharArrayValue paraNVarchar = new (["NVarchar value", "Varying NChar"]);
    string[] paraString = ["Hello", "Ballerina"];
    BooleanArrayValue paraBool = new ([true, false]);
    DateArrayValue paraDate = new (["2021-12-18", "2021-12-19"]);
    time:TimeOfDay time = {hour: 20, minute: 8, second: 12};
    TimeArrayValue paraTime = new ([time, time]);
    time:Civil datetime = {year: 2021, month: 12, day: 18, hour: 20, minute: 8, second: 12};
    DateTimeArrayValue paraDateTime = new ([datetime, datetime]);
    time:Utc timestampUtc = [12345600, 12];
    TimestampArrayValue paraTimestamp = new ([timestampUtc, timestampUtc]);
    byte[] byteArray1 = [1, 2, 3];
    byte[] byteArray2 = [4, 5, 6];
    BinaryArrayValue paraBinary = new ([byteArray1, byteArray2]);
    VarBinaryArrayValue paraVarBinary = new ([byteArray1, byteArray2]);
    int rowId = 1;
    datetime = {year: 2021, month: 12, day: 18, hour: 20, minute: 8, second: 12, utcOffset: {hours: 5, minutes: 30, seconds: 0d}};
    time:Civil[] paraTimestampWithTimezone = [datetime, datetime];
    time:TimeOfDay timeWithTimezone = {hour: 20, minute: 8, second: 12, utcOffset: {hours: 5, minutes: 30, seconds: 0d}};
    time:TimeOfDay[] paraTimeWithTimezone = [timeWithTimezone, timeWithTimezone];

    InOutParameter smallint_array = new(paraSmallint);
    InOutParameter int_array = new(paraInt);
    InOutParameter long_array = new(paraLong);
    InOutParameter float_array = new(paraFloat);
    InOutParameter double_array = new(paraDouble);
    InOutParameter real_array = new(paraReal);
    InOutParameter decimal_array = new(paraDecimal);
    InOutParameter numeric_array = new(paraNumeric);
    InOutParameter boolean_array = new(paraBool);
    InOutParameter char_array = new(paraChar);
    InOutParameter varchar_array = new(paraVarchar);
    InOutParameter nvarchar_array = new(paraNVarchar);
    InOutParameter string_array = new (paraString);
    InOutParameter date_array = new(paraDate);
    InOutParameter time_array = new(paraTime);
    InOutParameter datetime_array = new(paraDateTime);
    InOutParameter timestamp_array = new(paraTimestamp);
    InOutParameter time_tz_array = new(paraTimeWithTimezone);
    InOutParameter timestamp_tz_array = new(paraTimestampWithTimezone);

    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectArrayDataWithInoutParams (IN rowId INT, INOUT p_small_int_array SMALLINT ARRAY,
                                                INOUT p_int_array INTEGER ARRAY, INOUT p_real_array REAL ARRAY,
                                                INOUT p_numeric_array NUMERIC(6,2) ARRAY,
                                                INOUT p_nvarchar_array NVARCHAR(15) ARRAY,
                                                INOUT p_long_array BIGINT ARRAY,
                                                INOUT p_float_array FLOAT ARRAY,
                                                INOUT p_double_array DOUBLE ARRAY,
                                                INOUT p_decimal_array DECIMAL(6,2) ARRAY,
                                                INOUT p_boolean_array BOOLEAN ARRAY,
                                                INOUT p_char_array CHAR(15) ARRAY,
                                                INOUT p_varchar_array VARCHAR(100) ARRAY,
                                                INOUT p_string_array VARCHAR(20) ARRAY,
                                                INOUT p_date_array DATE ARRAY,
                                                INOUT p_time_array TIME ARRAY,
                                                INOUT p_timestamp_array timestamp ARRAY,
                                                INOUT p_datetime_array DATETIME ARRAY
                                                )
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT smallint_array INTO p_small_int_array FROM ProArrayTypes where row_id = rowId;
                SELECT int_array INTO p_int_array FROM ProArrayTypes where row_id = rowId;
                SELECT real_array INTO p_real_array FROM ProArrayTypes where row_id = rowId;
                SELECT numeric_array INTO p_numeric_array FROM ProArrayTypes where row_id = rowId;
                SELECT nvarchar_array INTO p_nvarchar_array FROM ProArrayTypes where row_id = rowId;
                SELECT long_array INTO p_long_array FROM ProArrayTypes where row_id = rowId;
                SELECT float_array INTO p_float_array FROM ProArrayTypes where row_id = rowId;
                SELECT double_array INTO p_double_array FROM ProArrayTypes where row_id = rowId;
                SELECT decimal_array INTO p_decimal_array FROM ProArrayTypes where row_id = rowId;
                SELECT boolean_array INTO p_boolean_array FROM ProArrayTypes where row_id = rowId;
                SELECT char_array INTO p_char_array FROM ProArrayTypes where row_id = rowId;
                SELECT varchar_array INTO p_varchar_array FROM ProArrayTypes where row_id = rowId;
                SELECT string_array INTO p_string_array FROM ProArrayTypes where row_id = rowId;
                SELECT date_array INTO p_date_array FROM ProArrayTypes where row_id = rowId;
                SELECT time_array INTO p_time_array FROM ProArrayTypes where row_id = rowId;
                SELECT timestamp_array INTO p_timestamp_array FROM ProArrayTypes where row_id = rowId;
                SELECT datetime_array INTO p_datetime_array FROM ProArrayTypes where row_id = rowId;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());

    ParameterizedCallQuery callProcedureQuery = `call SelectArrayDataWithInoutParams(${rowId}, ${smallint_array},
                                    ${int_array}, ${real_array}, ${numeric_array}, ${nvarchar_array}, ${long_array},
                                    ${float_array}, ${double_array}, ${decimal_array}, ${boolean_array},
                                    ${char_array}, ${varchar_array}, ${string_array}, ${date_array}, ${time_array},
                                    ${timestamp_array}, ${datetime_array})`;
    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    int[] smallIntArray = [12,232];
    int[] intArray = [1,2,3];
    float[] floatArray = [199.33,2399.1];
    float[] numericArray = [11.109999656677246,23.229999542236328];
    string[] nVarcharArray = ["Hello","Ballerina"];
    time:Civil[] civilArray = [{year:2017,month:2,day:3,hour:11,minute:53,second:0}, {year:2019,month:4,day:5,hour:12,minute:33,second:10}];
    test:assertEquals(smallint_array.get(IntArray), smallIntArray, "Small int array out parameter of procedure did not match.");
    test:assertEquals(int_array.get(IntArray), intArray, "Int array out parameter of procedure did not match.");
    test:assertEquals(real_array.get(FloatArray), floatArray, "Real array out parameter of procedure did not match.");
    test:assertEquals(numeric_array.get(FloatArray), numericArray, "Numeric array out parameter of procedure did not match.");
    test:assertEquals(nvarchar_array.get(StringArray), nVarcharArray, "Nvarchar array out parameter of procedure did not match.");
    test:assertEquals(datetime_array.get(CivilArray), civilArray, "Nvarchar array out parameter of procedure did not match.");
}

@test:Config {
    groups: ["procedures"]
}
function testCallWithAllArrayTypesOutParamsAsObjectValues() returns error? {
    SmallIntArrayOutParameter smallint_array = new;
    IntegerArrayOutParameter int_array = new;
    BigIntArrayOutParameter long_array = new;
    DoubleArrayOutParameter double_array = new;
    RealArrayOutParameter real_array = new;
    FloatArrayOutParameter float_array = new;
    DecimalArrayOutParameter decimal_array = new;
    NumericArrayOutParameter numeric_array = new;
    CharArrayOutParameter char_array = new;
    VarcharArrayOutParameter varchar_array = new;
    NVarcharArrayOutParameter nvarchar_array = new;
    BinaryArrayOutParameter binary_array = new;
    VarBinaryArrayOutParameter varbinaryArray = new;
    BooleanArrayOutParameter boolean_array = new;
    DateArrayOutParameter date_array = new;
    TimeArrayOutParameter time_array = new;
    DateTimeArrayOutParameter datetime_array = new;
    TimestampArrayOutParameter timestamp_array = new;
    ArrayOutParameter string_array = new;
    BitArrayOutParameter bit_array = new;
    TimeWithTimezoneArrayOutParameter time_tz_array = new;
    TimestampWithTimezoneArrayOutParameter timestamp_tz_array = new;
    int rowId = 1;

    ParameterizedQuery createProcedure = `
        CREATE PROCEDURE SelectArrayDataWithOutParams (IN rowId INT, OUT p_small_int_array SMALLINT ARRAY,
                                                OUT p_int_array INTEGER ARRAY, OUT p_real_array REAL ARRAY,
                                                OUT p_numeric_array NUMERIC(6,2) ARRAY,
                                                OUT p_nvarchar_array NVARCHAR(15) ARRAY,
                                                OUT p_long_array BIGINT ARRAY,
                                                OUT p_float_array FLOAT ARRAY,
                                                OUT p_double_array DOUBLE ARRAY,
                                                OUT p_decimal_array DECIMAL(6,2) ARRAY,
                                                OUT p_boolean_array BOOLEAN ARRAY,
                                                OUT p_char_array CHAR(15) ARRAY,
                                                OUT p_varchar_array VARCHAR(100) ARRAY,
                                                OUT p_string_array VARCHAR(20) ARRAY,
                                                OUT p_date_array DATE ARRAY,
                                                OUT p_time_array TIME ARRAY,
                                                OUT p_timestamp_array timestamp ARRAY,
                                                OUT p_datetime_array DATETIME ARRAY,
                                                OUT p_bit_array BIT(4) ARRAY,
                                                OUT p_time_tz_array TIME WITH TIME ZONE ARRAY,
                                                OUT p_timestamp_tz_array TIMESTAMP WITH TIME ZONE ARRAY,
                                                OUT p_binary_array BINARY(27) ARRAY
                                                )
            READS SQL DATA DYNAMIC RESULT SETS 2
            BEGIN ATOMIC
                SELECT smallint_array INTO p_small_int_array FROM ProArrayTypes where row_id = rowId;
                SELECT int_array INTO p_int_array FROM ProArrayTypes where row_id = rowId;
                SELECT real_array INTO p_real_array FROM ProArrayTypes where row_id = rowId;
                SELECT numeric_array INTO p_numeric_array FROM ProArrayTypes where row_id = rowId;
                SELECT nvarchar_array INTO p_nvarchar_array FROM ProArrayTypes where row_id = rowId;
                SELECT long_array INTO p_long_array FROM ProArrayTypes where row_id = rowId;
                SELECT float_array INTO p_float_array FROM ProArrayTypes where row_id = rowId;
                SELECT double_array INTO p_double_array FROM ProArrayTypes where row_id = rowId;
                SELECT decimal_array INTO p_decimal_array FROM ProArrayTypes where row_id = rowId;
                SELECT boolean_array INTO p_boolean_array FROM ProArrayTypes where row_id = rowId;
                SELECT char_array INTO p_char_array FROM ProArrayTypes where row_id = rowId;
                SELECT varchar_array INTO p_varchar_array FROM ProArrayTypes where row_id = rowId;
                SELECT string_array INTO p_string_array FROM ProArrayTypes where row_id = rowId;
                SELECT date_array INTO p_date_array FROM ProArrayTypes where row_id = rowId;
                SELECT time_array INTO p_time_array FROM ProArrayTypes where row_id = rowId;
                SELECT timestamp_array INTO p_timestamp_array FROM ProArrayTypes where row_id = rowId;
                SELECT datetime_array INTO p_datetime_array FROM ProArrayTypes where row_id = rowId;
                SELECT bit_array INTO p_bit_array FROM ProArrayTypes where row_id = rowId;
                SELECT time_tz_array INTO p_time_tz_array FROM ProArrayTypes where row_id = rowId;
                SELECT timestamp_tz_array INTO p_timestamp_tz_array FROM ProArrayTypes where row_id = rowId;
                SELECT binary_array INTO p_binary_array FROM ProArrayTypes where row_id = rowId;
            END
        `;
    validateProcedureResult(check createSqlProcedure(createProcedure),0,());

    ParameterizedCallQuery callProcedureQuery = `call SelectArrayDataWithOutParams(${rowId}, ${smallint_array},
                                    ${int_array}, ${real_array}, ${numeric_array}, ${nvarchar_array}, ${long_array},
                                    ${float_array}, ${double_array}, ${decimal_array}, ${boolean_array},
                                    ${char_array}, ${varchar_array}, ${string_array}, ${date_array}, ${time_array},
                                    ${timestamp_array}, ${datetime_array}, ${bit_array}, ${time_tz_array},
                                    ${timestamp_tz_array}, ${binary_array})`;
    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();

    MockClient dbClient = check new (url = proceduresDB, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT * FROM ProArrayTypes WHERE row_id = 1");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();
    int[] smallIntArray = [12,232];
    int[] intArray = [1,2,3];
    float[] realArray = [199.33,2399.1];
    float[] realArrayInFloat = [199.3300018310547,2399.10009765625];
    decimal[] realArrayInDecimal = [199.33,2399.1];
    float[] floatArray = [245.23,5559.49,8796.123];
    decimal[] numericArray = [11.11,23.23];
    float[] numericArrayInFloat = [11.109999656677246,23.229999542236328];
    decimal[] decimalArray = [245.12,5559.12,8796.92];
    float[] decimalArrayInFloat = [245.1199951171875,5559.1201171875,8796.919921875];
    float[] doubleArray = [245.22999572753906,5559.490234375,8796.123046875];
    string[] nVarcharArray = ["Hello","Ballerina"];
    string[] charArray = ["Hello          ","Ballerina      "];
    string[] varcharArray = ["Hello","Ballerina"];
    int[] longArray = [100000000,200000000,300000000];
    boolean[] booleanArray = [true,false,true];
    byte[][] byteArray = [[128],[128],[0]];
    byte[][] binaryArray = [[119,115,111,50,32,98,97,108,108,101,114,105,110,97,32,98,105,110,97,114,121,32,116,101,115,116,139]];
    time:Civil[] civilArray = [{year:2017,month:2,day:3,hour:11,minute:53,second:0},
    {year:2019,month:4,day:5,hour:12,minute:33,second:10}];
    string[] civilArrayInString = ["2017-02-03 11:53:00.0","2019-04-05 12:33:10.0"];
    time:Date[] dateArray = [{"year":2017,"month":2,"day":3},{"year":2017,"month":2,"day":3}];
    string[] dateArrayInString = ["2017-02-03","2017-02-03"];
    time:TimeOfDay[] timeOfDayArray = [{"hour":11,"minute":22,"second":42}, {"hour":12,"minute":23,"second":45}];
    string[] timeOfDayArrayInstring = ["11:22:42","12:23:45"];
    time:TimeOfDay[] timetzArray = [{"utcOffset":{"hours":6,"minutes":30},"hour":16,"minute":33,"second":55,"timeAbbrev":"+06:30"},{"utcOffset":{"hours":4,"minutes":30},"hour":16,"minute":33,"second":55,"timeAbbrev":"+04:30"}];
    time:Civil[] timestamptzArray = [{"utcOffset":{"hours":-8,"minutes":0},"timeAbbrev":"-08:00","year":2017,"month":1,"day":25,"hour":16,"minute":33,"second":55},{"utcOffset":{"hours":-5,"minutes":0},"timeAbbrev":"-05:00","year":2017,"month":1,"day":25,"hour":16,"minute":33,"second":55}];
    string[] timestamptzArrayInString = ["2017-01-25T16:33:55-08:00","2017-01-25T16:33:55-05:00"];
    test:assertEquals(smallint_array.get(IntArray), smallIntArray, "Small int array out parameter of " +
    "procedure did not match.");
    test:assertEquals(smallint_array.get(StringArray), ["12","232"], "Small int array out parameter of " +
    "procedure did not match.");
    test:assertEquals(int_array.get(IntArray), intArray, "Int array out parameter of procedure did not match.");
    test:assertEquals(int_array.get(StringArray), ["1","2","3"], "Int array out parameter of procedure did not match.");
    test:assertEquals(real_array.get(FloatArray), realArrayInFloat, "Real array out parameter of procedure did not match.");
    test:assertEquals(real_array.get(IntArray), [199,2399], "Real array out parameter of procedure did not match.");
    test:assertEquals(real_array.get(StringArray), ["199.33","2399.1"], "Real array out parameter of procedure did not match.");
    test:assertEquals(real_array.get(DecimalArray), realArrayInDecimal, "Real array out parameter of procedure did not match.");
    test:assertEquals(numeric_array.get(FloatArray), numericArrayInFloat, "Numeric array out parameter " +
    "of procedure did not match.");
    test:assertEquals(numeric_array.get(StringArray), ["11.11","23.23"], "Numeric array out parameter " +
    "of procedure did not match.");
    test:assertEquals(numeric_array.get(IntArray), [11,23], "Numeric array out parameter " +
    "of procedure did not match.");
    test:assertEquals(numeric_array.get(DecimalArray), numericArray, "Numeric array out parameter " +
    "of procedure did not match.");
    test:assertEquals(nvarchar_array.get(StringArray), nVarcharArray, "Nvarchar array out parameter " +
    "of procedure did not match.");
    test:assertEquals(long_array.get(IntArray), longArray, "Long array out parameter of procedure did not match.");
    test:assertEquals(long_array.get(StringArray), ["100000000","200000000","300000000"], "Long array out parameter of " +
    "procedure did not match.");
    test:assertEquals(float_array.get(IntArray), [245,5559,8796], "Float array out parameter " +
    "of procedure did not match.");
    test:assertEquals(float_array.get(StringArray), ["245.23","5559.49","8796.123"], "Float array out parameter " +
    "of procedure did not match.");
    test:assertEquals(double_array.get(FloatArray), doubleArray, "Double array out parameter " +
    "of procedure did not match.");
    test:assertEquals(boolean_array.get(BooleanArray), booleanArray, "Boolean array out parameter " +
    "of procedure did not match.");
    test:assertEquals(boolean_array.get(IntArray), [1,0,1], "Boolean array out parameter " +
    "of procedure did not match.");
    test:assertEquals(boolean_array.get(StringArray), ["true","false","true"], "Boolean array out parameter " +
        "of procedure did not match.");
    test:assertEquals(decimal_array.get(DecimalArray), decimalArray, "Decimal array out parameter " +
    "of procedure did not match.");
    test:assertEquals(decimal_array.get(StringArray), ["245.12","5559.12","8796.92"], "Decimal array out parameter " +
    "of procedure did not match.");
    test:assertEquals(decimal_array.get(IntArray), [245,5559,8796], "Decimal array out parameter " +
    "of procedure did not match.");
    test:assertEquals(decimal_array.get(FloatArray), decimalArrayInFloat, "Decimal array out parameter " +
    "of procedure did not match.");
    test:assertEquals(char_array.get(StringArray), charArray, "Char array out parameter of procedure did not match.");
    test:assertEquals(varchar_array.get(StringArray), varcharArray, "Varchar array out parameter " +
    "of procedure did not match.");
    test:assertEquals(timestamp_array.get(UtcArray), civilArray, "Timestamp array out parameter " +
    "of procedure did not match.");
    test:assertEquals(timestamp_array.get(StringArray), civilArrayInString, "String timestamp array out parameter " +
        "of procedure did not match.");
    test:assertEquals(date_array.get(DateArray), dateArray, "Date array out parameter of procedure did not match.");
    test:assertEquals(date_array.get(StringArray), dateArrayInString, "String date array out parameter of procedure " +
    "did not match.");
    test:assertEquals(datetime_array.get(CivilArray), civilArray, "Date time array out parameter " +
    "of procedure did not match.");
    test:assertEquals(datetime_array.get(StringArray), civilArrayInString, "String date time array out parameter " +
    "of procedure did not match.");
    test:assertEquals(time_array.get(TimeOfDayArray), timeOfDayArray, "Time array out parameter " +
    "of procedure did not match.");
    test:assertEquals(time_array.get(StringArray), timeOfDayArrayInstring, "String time array out parameter " +
    "of procedure did not match.");
    test:assertEquals(bit_array.get(ByteArray), byteArray, "Bit array out parameter " +
    "of procedure did not match.");
    test:assertFalse((bit_array.get(StringArray) is Error));
    test:assertEquals(time_tz_array.get(TimeOfDayArray), timetzArray, "Time with timezone array out parameter " +
    "of procedure did not match.");
    test:assertEquals(time_tz_array.get(StringArray), ["16:33:55+06:30","16:33:55+04:30"], "Timestamp with timezone array out " +
    "parameter of procedure did not match.");
    test:assertEquals(timestamp_tz_array.get(CivilArray), timestamptzArray, "Timestamp with timezone array out " +
    "parameter of procedure did not match.");
    test:assertEquals(timestamp_tz_array.get(StringArray), timestamptzArrayInString, "Timestamp with timezone array out " +
    "parameter of procedure did not match.");
    test:assertEquals(binary_array.get(ByteArray), binaryArray, "Timestamp with timezone array out parameter of " +
    "procedure did not match.");
    test:assertFalse((binary_array.get(StringArray) is Error));
}

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCallWithAllArrayTypesOutParamsAsObjectValues]
}
function negativeOutParamsTest() returns error? {
    SmallIntArrayOutParameter smallint_array = new;
    IntegerArrayOutParameter int_array = new;
    BigIntArrayOutParameter long_array = new;
    DoubleArrayOutParameter double_array = new;
    RealArrayOutParameter real_array = new;
    FloatArrayOutParameter float_array = new;
    DecimalArrayOutParameter decimal_array = new;
    NumericArrayOutParameter numeric_array = new;
    CharArrayOutParameter char_array = new;
    VarcharArrayOutParameter varchar_array = new;
    NVarcharArrayOutParameter nvarchar_array = new;
    BinaryArrayOutParameter binaryArray = new;
    VarBinaryArrayOutParameter varbinaryArray = new;
    BooleanArrayOutParameter boolean_array = new;
    DateArrayOutParameter date_array = new;
    TimeArrayOutParameter time_array = new;
    DateTimeArrayOutParameter datetime_array = new;
    TimestampArrayOutParameter timestamp_array = new;
    ArrayOutParameter string_array = new;
    BitArrayOutParameter bit_array = new;
    TimeWithTimezoneArrayOutParameter time_tz_array = new;
    TimestampWithTimezoneArrayOutParameter timestamp_tz_array = new;
    BinaryArrayOutParameter binary_array = new;
    int rowId = 1;
    ParameterizedCallQuery callProcedureQuery = `call SelectArrayDataWithOutParams(${rowId}, ${smallint_array},
                                        ${int_array}, ${real_array}, ${numeric_array}, ${nvarchar_array}, ${long_array},
                                        ${float_array}, ${double_array}, ${decimal_array}, ${boolean_array},
                                        ${char_array}, ${varchar_array}, ${string_array}, ${date_array}, ${time_array},
                                        ${timestamp_array}, ${datetime_array}, ${bit_array}, ${time_tz_array},
                                        ${timestamp_tz_array}, ${binary_array})`;
    ProcedureCallResult ret = check getProcedureCallResultFromMockClient(callProcedureQuery);
    check ret.close();
    byte[][]|Error result = smallint_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:SmallInt array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = int_array.get(ByteArray);

    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for SQL Date data " +
        "type:Integer array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = real_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Real array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = numeric_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Numeric array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = nvarchar_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:NVarchar array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = long_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:BigInt array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = float_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Float array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = double_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Double array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = decimal_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Decimal array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = char_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Char array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = varchar_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Varchar array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = string_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for SQL Date data type."));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = date_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Date array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = time_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Time array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = timestamp_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Timestamp array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = datetime_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:DateTime array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = timestamp_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for " +
        "SQL Date data type:Timestamp array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = time_tz_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for SQL Date data " +
        "type:TimeWithTimezone array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = timestamp_tz_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for SQL Date data " +
        "type:TimestampWithTimezone array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    result = boolean_array.get(ByteArray);
    if (result is Error) {
        test:assertTrue(result.toString().includes("Unsupported Ballerina type:byte[][] for SQL Date data " +
        "type:Boolean array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

    float[]|Error output = binary_array.get(FloatArray);
    if (output is Error) {
        test:assertTrue(output.toString().includes("Unsupported Ballerina type:float[] for SQL Date " +
        "data type:Binary array"));
    } else {
        test:assertFail("Result is not mismatch");
    }

}

function getProcedureCallResultFromMockClient(ParameterizedCallQuery sqlQuery) returns ProcedureCallResult|error {
    MockClient dbClient = check new (url = proceduresDB, user = user, password = password);
    ProcedureCallResult result = check dbClient->call(sqlQuery);
    check dbClient.close();
    return result;
}

function createSqlProcedure(ParameterizedQuery sqlQuery) returns ExecutionResult|Error {
    MockClient dbClient = check new (url = proceduresDB, user = user, password = password);
    ExecutionResult result = check dbClient->execute(sqlQuery);
    check dbClient.close();
    return result;
}

isolated function validateProcedureResult(ExecutionResult result, int rowCount, int? lastId = ()) {
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
