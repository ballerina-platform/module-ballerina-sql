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
    dependsOn: [testCallWithStringTypesInoutParams,testCreateProcedures8]
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
    int[] intArr = [1, 2];
    ArrayValue intArrayVal = new(intArr);
    string[] strArr = ["Hello", "Ballerina"];
    ArrayValue strArrayVal = new(strArr);
    boolean[] booArr = [true, false, true];
    ArrayValue booArrayVal = new(booArr);
    float[] floArr = [245.23, 5559.49, 8796.123];
    ArrayValue floArrayVal = new(floArr);
    decimal[] decArr = [245, 5559, 8796];
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

    string clobType = "very long text";
    var varBinaryType = "77736f322062616c6c6572696e612062696e61727920746573742e".toBytes();
    time:Civil dateTimeRecord = {year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55};

    test:assertEquals(paraClob.get(string), clobType, "Clob out parameter of procedure did not match.");
    test:assertEquals(paraVarBinary.get(byte), varBinaryType, "VarBinary out parameter of procedure did not match.");
    test:assertEquals(paraDateTime.get(time:Civil), dateTimeRecord, "DateTime out parameter of procedure did not match.");
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

type IntArray int[];
type StringArray string[];

@test:Config {
    groups: ["procedures"],
    dependsOn: [testCreateProcedures7]
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
    dependsOn: [testCreateProcedures7]
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
    groups: ["procedures"]
}
function testCreateProcedures7() returns error? {
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
    groups: ["procedures"],
    dependsOn: [testCreateProcedures7]
}
function testCreateProcedures8() returns error? {
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

isolated function validateProcedureResult(ExecutionResult|Error result, int rowCount, int? lastId = ()) {
    if(result is Error){
        test:assertFail("Procedure creation failed");
    } else {
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
}
