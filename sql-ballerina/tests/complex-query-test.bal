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

import ballerina/lang.'string as strings;
import ballerina/test;
import ballerina/time;

string complexQueryDb = urlPrefix + "9008/querycomplexparams";

@test:BeforeGroups {
 	value: ["query-complex-params"]
} 
function initQueryComplexContainer() {
 	initializeDockerContainer("sql-query-complex", "querycomplexparams", "9008", "query", "complex-test-data.sql");
}

@test:AfterGroups {
 	value: ["query-complex-params"]
} 
function cleanQueryComplexContainer() {
	cleanDockerContainer("sql-query-complex");
}

type SelectTestAlias record {
    int INT_TYPE;
    int LONG_TYPE;
    float DOUBLE_TYPE;
    boolean BOOLEAN_TYPE;
    string STRING_TYPE;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}

function testGetPrimitiveTypes() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type,"
        + "boolean_type, string_type from DataTable WHERE row_id = 1");
    record {|record {} value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();
    record {}? value = data?.value;
    checkpanic dbClient.close();

    SelectTestAlias expectedData = {
        INT_TYPE: 1,
        LONG_TYPE: 9223372036854774807,
        DOUBLE_TYPE: 2139095039,
        BOOLEAN_TYPE: true,
        STRING_TYPE: "Hello"
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testToJson() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type, boolean_type, string_type from DataTable WHERE row_id = 1");
    record {|record {} value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();
    record {}? value = data?.value;
    json retVal = checkpanic value.cloneWithType(json);
    SelectTestAlias expectedData = {
        INT_TYPE: 1,
        LONG_TYPE: 9223372036854774807,
        DOUBLE_TYPE: 2139095039,
        BOOLEAN_TYPE: true,
        STRING_TYPE: "Hello"
    };
    json|error expectedDataJson = expectedData.cloneWithType(json);
    if (expectedDataJson is json) {
        test:assertEquals(retVal, expectedDataJson, "Expected JSON did not match.");
    } else {
        test:assertFail("Error in cloning record to JSON" + expectedDataJson.message());
    }

    checkpanic dbClient.close();
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testToJsonComplexTypes() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT blob_type,clob_type,binary_type from" +
        " ComplexTypes where row_id = 1");
    record {|record {} value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();
    record {}? value = data?.value;
    checkpanic dbClient.close();

    var complexStringType = {
        BLOB_TYPE: "wso2 ballerina blob test.".toBytes(),
        CLOB_TYPE: "very long text",
        BINARY_TYPE: "wso2 ballerina binary test.".toBytes()
    };
    test:assertEquals(value, complexStringType, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testComplexTypesNil() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT blob_type,clob_type,binary_type from " +
        " ComplexTypes where row_id = 2");
    record {|record {} value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();
    record {}? value = data?.value;
    checkpanic dbClient.close();
    var complexStringType = {
        BLOB_TYPE: (),
        CLOB_TYPE: (),
        BINARY_TYPE: ()
    };
    test:assertEquals(value, complexStringType, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testArrayRetrieval() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT int_type, int_array, long_type, long_array, " +
        "boolean_type, string_type, string_array, boolean_array " +
        "from MixTypes where row_id =1");
    record {|record {} value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();
    record {}? value = data?.value;
    checkpanic dbClient.close();

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

type TestTypeData record {
    int int_type;
    int[] int_array;
    int long_type;
    int[] long_array;
    boolean boolean_type;
    string string_type;
    string[] string_array;
    boolean[] boolean_array;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testComplexWithStructDef() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT int_type, int_array, long_type, long_array, "
        + "boolean_type, string_type, boolean_array, string_array "
        + "from MixTypes where row_id =1", TestTypeData);
    record {|record {} value;|}? data = checkpanic streamData.next();
    checkpanic streamData.close();
    record {}? value = data?.value;
    checkpanic dbClient.close();
    TestTypeData mixTypesExpected = {
        int_type: 1,
        int_array: [1, 2, 3],
        long_type: 9223372036854774807,
        long_array:[100000000, 200000000, 300000000],
        boolean_type: true,
        string_type: "Hello",
        boolean_array: [true, false, true],
        string_array: ["Hello", "Ballerina"]
    };
    test:assertEquals(value, mixTypesExpected, "Expected record did not match.");
}

type ResultMap record {
    int[] INT_ARRAY;
    int[] LONG_ARRAY;
    boolean[] BOOLEAN_ARRAY;
    string[] STRING_ARRAY;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testMultipleRecoredRetrieval() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT int_array, long_array, boolean_array," +
        "string_array from ArrayTypes", ResultMap);

    ResultMap mixTypesExpected = {
        INT_ARRAY: [1, 2, 3],
        LONG_ARRAY: [100000000, 200000000, 300000000],
        STRING_ARRAY: ["Hello", "Ballerina"],
        BOOLEAN_ARRAY: [true, false, true]
    };

    ResultMap? mixTypesActual = ();
    int counter = 0;
    error? e = streamData.forEach(function (record {} value) {
        if (value is ResultMap && counter == 0) {
            mixTypesActual = value;
        }
        counter = counter + 1;
    });
    if (e is error) {
        test:assertFail("Error when iterating through records " + e.message());
    }
    test:assertEquals(mixTypesActual, mixTypesExpected, "Expected record did not match.");
    test:assertEquals(counter, 4);
    checkpanic dbClient.close();

}

type ResultDates record {
    string DATE_TYPE;
    string TIME_TYPE;
    string TIMESTAMP_TYPE;
    string DATETIME_TYPE;
    string TIME_TZ_TYPE;
    string TIMESTAMP_TZ_TYPE;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testDateTime() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> queryResult = dbClient->query("SELECT date_type, time_type, timestamp_type, datetime_type"
       + ", time_tz_type, timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates);
    record{| record{} value; |}? data =  check queryResult.next();
    record{}? value = data?.value;
    check dbClient.close();

    string dateTypeString = "2017-05-23";
    string timeTypeString = "14:15:23";
    string timestampTypeString = "2017-01-25 16:33:55.0";
    string timeWithTimezone = "16:33:55+06:30";
    string timestampWithTimezone = "2017-01-25T16:33:55-08:00";

    ResultDates expected = {
        DATE_TYPE: dateTypeString,
        TIME_TYPE: timeTypeString,
        TIMESTAMP_TYPE: timestampTypeString,
        DATETIME_TYPE: timestampTypeString,
        TIME_TZ_TYPE: timeWithTimezone,
        TIMESTAMP_TZ_TYPE: timestampWithTimezone
    };
    test:assertEquals(value, expected, "Expected record did not match.");
}

type ResultDates2 record {
    time:Date date_type;
    time:TimeOfDay time_type;
    time:Civil timestamp_type;
    time:Civil datetime_type;
    time:TimeOfDay time_tz_type;
    time:Civil timestamp_tz_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testDateTime2() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> queryResult = dbClient->query("SELECT date_type, time_type, timestamp_type, datetime_type, "
            + "time_tz_type, timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates2);
    record{| record{} value; |}? data =  check queryResult.next();
    record{}? value = data?.value;
    check dbClient.close();

    time:Date dateTypeRecord = {year: 2017, month: 5, day: 23};
    time:TimeOfDay timeTypeRecord = {hour: 14, minute: 15, second:23};
    time:Civil timestampTypeRecord = {year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55};
    time:TimeOfDay timeWithTimezone = {utcOffset: {hours: 6, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+06:30"};
    time:Civil timestampWithTimezone = {utcOffset: {hours: -8, minutes: 0}, timeAbbrev: "-08:00", year:2017,
                                        month:1, day:25, hour: 16, minute: 33, second:55};

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

type RandomType record {|
    int x;
|};

type ResultDates3 record {
    RandomType date_type;
    RandomType time_type;
    RandomType timestamp_type;
    RandomType datetime_type;
    RandomType time_tz_type;
    RandomType timestamp_tz_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testDateTime3() returns error? {
    stream<record{}, error?> queryResult;
    record {|record {} value;|} | error? result;
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);

    queryResult = dbClient->query("SELECT date_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Date type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Date type.");

    queryResult = dbClient->query("SELECT time_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Time type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Time type.");

    queryResult = dbClient->query("SELECT timestamp_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Timestamp type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Timestamp type.");

    queryResult = dbClient->query("SELECT datetime_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Datetime type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Datetime type.");

    queryResult = dbClient->query("SELECT time_tz_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Time with Timezone type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Time with Timezone type.");
    
    queryResult = dbClient->query("SELECT timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Timestamp with Timezone type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Timestamp with Timezone type.");
    check dbClient.close();
}

type ResultSetTestAlias record {
    int INT_TYPE;
    int LONG_TYPE;
    string FLOAT_TYPE;
    float DOUBLE_TYPE;
    boolean BOOLEAN_TYPE;
    string STRING_TYPE;
    int DT2INT_TYPE;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testColumnAlias() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> queryResult = dbClient->query("SELECT dt1.int_type, dt1.long_type, dt1.float_type," +
           "dt1.double_type,dt1.boolean_type, dt1.string_type,dt2.int_type as dt2int_type from DataTable dt1 " +
           "left join DataTableRep dt2 on dt1.row_id = dt2.row_id WHERE dt1.row_id = 1;", ResultSetTestAlias);
    ResultSetTestAlias expectedData = {
        INT_TYPE: 1,
        LONG_TYPE: 9223372036854774807,
        FLOAT_TYPE: "123.34",
        DOUBLE_TYPE: 2139095039,
        BOOLEAN_TYPE: true,
        STRING_TYPE: "Hello",
        DT2INT_TYPE: 100
    };
    int counter = 0;
    error? e = queryResult.forEach(function (record{} value) {
        if (value is ResultSetTestAlias) {
            test:assertEquals(value, expectedData, "Expected record did not match.");
            counter = counter + 1;
        } else{
            test:assertFail("Expected data type is ResultSetTestAlias");
        }
    });
    if(e is error) {
        test:assertFail("Query failed");
    }
    test:assertEquals(counter, 1, "Expected only one data row.");
    checkpanic dbClient.close();
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testQueryRowId() {
    MockClient dbClient = checkpanic new (url = complexQueryDb, user = user, password = password);
    ExecutionResult result = checkpanic dbClient->execute("SET DATABASE SQL SYNTAX ORA TRUE");
    stream<record{}, error?> streamData = dbClient->query("SELECT ROWNUM, int_array, long_array, boolean_array," +
         "string_array from ArrayTypes");

    record{} mixTypesExpected = {
        "ROWNUM": 1,
        "INT_ARRAY": [1, 2, 3],
        "LONG_ARRAY": [100000000, 200000000, 300000000],
        "BOOLEAN_ARRAY": [true, false, true],
        "STRING_ARRAY": ["Hello", "Ballerina"]
    };

    record{}? mixTypesActual = ();
    int counter = 0;
    error? e = streamData.forEach(function (record {} value) {
        if (counter == 0) {
            mixTypesActual = value;
        }
        counter = counter + 1;
    });
    if (e is error) {
        test:assertFail("Query failed");
    }
    test:assertEquals(mixTypesActual, mixTypesExpected, "Expected record did not match.");
    test:assertEquals(counter, 4);
    checkpanic dbClient.close();
}
