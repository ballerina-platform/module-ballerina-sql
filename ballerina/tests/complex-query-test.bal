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

string complexQueryDb = urlPrefix + "9008/querycomplexparams";

@test:BeforeGroups {
    value: ["query-complex-params"]
}
function initQueryComplexContainer() returns error? {
    check initializeDockerContainer("sql-query-complex", "querycomplexparams", "9008", "query", "complex-test-data.sql");
}

@test:AfterGroups {
    value: ["query-complex-params"]
}
function cleanQueryComplexContainer() returns error? {
    check cleanDockerContainer("sql-query-complex");
}

type SelectTestAlias record {
    int int_type;
    int long_type;
    float double_type;
    boolean boolean_type;
    string string_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testGetPrimitiveTypes() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type,"
        + "boolean_type, string_type from DataTable WHERE row_id = 1", SelectTestAlias);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
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
    groups: ["query", "query-complex-params"]
}
function testGetPrimitiveTypes2() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<SelectTestAlias, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type,"
        + "boolean_type, string_type from DataTable WHERE row_id = 1", SelectTestAlias);
    record {|SelectTestAlias value;|}? data = check streamData.next();
    check streamData.close();
    SelectTestAlias? value = data?.value;
    check dbClient.close();

    SelectTestAlias expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        double_type: 2139095039,
        boolean_type: true,
        string_type: "Hello"
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
    test:assertTrue(value is SelectTestAlias, "Received value type is different.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testGetPrimitiveTypes3() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<SelectTestAlias, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type,"
        + "boolean_type, string_type from DataTable WHERE row_id = 1");
    record {|SelectTestAlias value;|}? data = check streamData.next();
    check streamData.close();
    SelectTestAlias? value = data?.value;
    check dbClient.close();

    SelectTestAlias expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        double_type: 2139095039,
        boolean_type: true,
        string_type: "Hello"
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
    test:assertTrue(value is SelectTestAlias, "Received value type is different.");
}

type SelectTestAlias2 record {
    int int_type;
    int long_type;
    float double_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testGetPrimitiveTypesLessFields() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<SelectTestAlias2, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type,"
        + "boolean_type, string_type from DataTable WHERE row_id = 1", SelectTestAlias2);
    record {|SelectTestAlias2 value;|}? data = check streamData.next();
    check streamData.close();
    SelectTestAlias2? value = data?.value;
    check dbClient.close();

    var expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        double_type: 2.139095039E9,
        BOOLEAN_TYPE: true,
        STRING_TYPE: "Hello"
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
    test:assertTrue(value is SelectTestAlias2, "Received value type is different.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testToJson() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type, boolean_type, string_type from DataTable WHERE row_id = 1", SelectTestAlias);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    json retVal = check value.cloneWithType(json);
    SelectTestAlias expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        double_type: 2139095039,
        boolean_type: true,
        string_type: "Hello"
    };
    json|error expectedDataJson = expectedData.cloneWithType(json);
    if expectedDataJson is json {
        test:assertEquals(retVal, expectedDataJson, "Expected JSON did not match.");
    } else {
        test:assertFail("Error in cloning record to JSON" + expectedDataJson.message());
    }

    check dbClient.close();
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testToJsonComplexTypes() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT blob_type,clob_type,binary_type from" +
        " ComplexTypes where row_id = 1");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();

    var complexStringType = {
        BLOB_TYPE: "wso2 ballerina blob test.".toBytes(),
        CLOB_TYPE: "very long text",
        BINARY_TYPE: "wso2 ballerina binary test.".toBytes()
    };
    test:assertEquals(value, complexStringType, "Expected record did not match.");
    test:assertTrue(data is record {|record {} value;|}, "Received value type is different.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testComplexTypesNil() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> streamData = dbClient->query("SELECT blob_type, clob_type, binary_type, other_type, uuid_type from " + 
        " ComplexTypes where row_id = 2");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
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
    groups: ["query", "query-complex-params"]
}
function testArrayRetrieval() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> streamData = dbClient->query("SELECT int_type, int_array, long_type, long_array, " + 
        "boolean_type, string_type, string_array, boolean_array " + 
        "from MixTypes where row_id =1");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();

    float[] doubleTypeArray = [245.23, 5559.49, 8796.123];
    var mixTypesExpected = {
        INT_TYPE: 1,
        INT_ARRAY: [1, 2, 3],
        LONG_TYPE: 9223372036854774807,
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
    json json_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testComplexWithStructDef() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> streamData = dbClient->query("SELECT int_type, int_array, long_type, long_array, " 
        + "boolean_type, string_type, boolean_array, string_array, json_type " 
        + "from MixTypes where row_id =1", TestTypeData);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();
    TestTypeData mixTypesExpected = {
        int_type: 1,
        int_array: [1, 2, 3],
        long_type: 9223372036854774807,
        long_array: [100000000, 200000000, 300000000],
        boolean_type: true,
        string_type: "Hello",
        boolean_array: [true, false, true],
        string_array: ["Hello", "Ballerina"],
        json_type: [1, 2, 3]
    };
    test:assertEquals(value, mixTypesExpected, "Expected record did not match.");
}

type ResultMap record {
    int[] int_array;
    int[] long_array;
    boolean[] boolean_array;
    string[] string_array;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testMultipleRecordRetrieval() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> streamData = dbClient->query("SELECT int_array, long_array, boolean_array," + 
        "string_array from ArrayTypes", ResultMap);

    ResultMap mixTypesExpected = {
        int_array: [1, 2, 3],
        long_array: [100000000, 200000000, 300000000],
        string_array: ["Hello", "Ballerina"],
        boolean_array: [true, false, true]
    };

    ResultMap? mixTypesActual = ();
    int counter = 0;
    error? e = streamData.forEach(function(record {} value) {
        if value is ResultMap && counter == 0 {
            mixTypesActual = value;
        }
        counter = counter + 1;
    });
    if e is error {
        test:assertFail("Error when iterating through records " + e.message());
    }
    test:assertEquals(mixTypesActual, mixTypesExpected, "Expected record did not match.");
    test:assertEquals(counter, 4);
    check dbClient.close();

}

type ResultDates record {
    string date_type;
    string time_type;
    string timestamp_type;
    string datetime_type;
    string time_tz_type;
    string timestamp_tz_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testDateTime() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> queryResult = dbClient->query("SELECT date_type, time_type, timestamp_type, datetime_type" 
        + ", time_tz_type, timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates);
    record {|record {} value;|}? data = check queryResult.next();
    record {}? value = data?.value;
    check dbClient.close();

    string dateTypeString = "2017-05-23";
    string timeTypeString = "14:15:23";
    string timestampTypeString = "2017-01-25 16:33:55.0";
    string timeWithTimezone = "16:33:55+06:30";
    string timestampWithTimezone = "2017-01-25T16:33:55-08:00";

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
    stream<record {}, error?> queryResult = dbClient->query("SELECT date_type, time_type, timestamp_type, datetime_type, " 
            + "time_tz_type, timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates2);
    record {|record {} value;|}? data = check queryResult.next();
    record {}? value = data?.value;
    check dbClient.close();

    time:Date dateTypeRecord = {year: 2017, month: 5, day: 23};
    time:TimeOfDay timeTypeRecord = {hour: 14, minute: 15, second: 23};
    time:Civil timestampTypeRecord = {year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55};
    time:TimeOfDay timeWithTimezone = {utcOffset: {hours: 6, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+06:30"};
    time:Civil timestampWithTimezone = {
        utcOffset: {hours: -8, minutes: 0},
        timeAbbrev: "-08:00",
        year: 2017,
        month: 1,
        day: 25,
        hour: 16,
        minute: 33,
        second: 55
    };

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
    stream<record {}, error?> queryResult;
    record {|record {} value;|}|error? result;
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);

    queryResult = dbClient->query("SELECT date_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Expected for Date type.");
    test:assertEquals((<error>result).message(),
        "Error when iterating the SQL result. The ballerina type expected for 'SQL Date' type is 'time:Date' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    queryResult = dbClient->query("SELECT time_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Expected for Time type.");
    test:assertEquals((<error>result).message(),
        "Error when iterating the SQL result. The ballerina type expected for 'SQL Time' type is 'time:TimeOfDay' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    queryResult = dbClient->query("SELECT timestamp_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Expected for Timestamp type.");
    test:assertEquals((<error>result).message(),
        "Error when iterating the SQL result. The ballerina type expected for 'SQL Timestamp' type is 'time:Civil' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    queryResult = dbClient->query("SELECT datetime_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Expected for Datetime type.");
    test:assertEquals((<error>result).message(),
        "Error when iterating the SQL result. The ballerina type expected for 'SQL Timestamp' type is 'time:Civil' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    queryResult = dbClient->query("SELECT time_tz_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Expected for Time with Timezone type.");
    test:assertEquals((<error>result).message(),
        "Error when iterating the SQL result. The ballerina type expected for 'SQL Time with Timezone' type is 'time:TimeOfDay' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    queryResult = dbClient->query("SELECT timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Expected for Timestamp with Timezone type.");
    test:assertEquals((<error>result).message(),
        "Error when iterating the SQL result. The ballerina type expected for 'SQL Timestamp with Timezone' type is 'time:Civil' but found type 'RandomType'.",
        "Wrong Error Message for Date type.");

    check dbClient.close();
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testDateTime4() returns error? {
    time:Civil timestampWithTimezone = {
            utcOffset: {hours: -8, minutes: 0},
            timeAbbrev: "-08:00",
            year: 2017,
            month: 1,
            day: 25,
            hour: 16,
            minute: 33,
            second: 55
    };
    time:Utc timeUtc = check time:utcFromCivil(timestampWithTimezone);

    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    time:Utc retrievedTimeUtc = check dbClient->queryRow(`
        SELECT timestamp_tz_type from DateTimeTypes where row_id = 1
    `);
    check dbClient.close();

    test:assertEquals(retrievedTimeUtc, timeUtc, "Expected UTC timestamp did not match.");
}

type ResultSetTestAlias record {
    int int_type;
    int long_type;
    string float_type;
    float double_type;
    boolean boolean_type;
    string string_type;
    int dt2int_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testColumnAlias() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> queryResult = dbClient->query("SELECT dt1.int_type, dt1.long_type, dt1.float_type," + 
            "dt1.double_type,dt1.boolean_type, dt1.string_type,dt2.int_type as dt2int_type from DataTable dt1 " + 
            "left join DataTableRep dt2 on dt1.row_id = dt2.row_id WHERE dt1.row_id = 1;", ResultSetTestAlias);
    ResultSetTestAlias expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        float_type: "123.34",
        double_type: 2139095039,
        boolean_type: true,
        string_type: "Hello",
        dt2int_type: 100
    };
    int counter = 0;
    error? e = queryResult.forEach(function(record {} value) {
        if value is ResultSetTestAlias {
            test:assertEquals(value, expectedData, "Expected record did not match.");
            counter = counter + 1;
        } else {
            test:assertFail("Expected data type is ResultSetTestAlias");
        }
    });
    if e is error {
        test:assertFail("Query failed");
    }
    test:assertEquals(counter, 1, "Expected only one data row.");
    check dbClient.close();
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testQueryRowId() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    ExecutionResult result = check dbClient->execute("SET DATABASE SQL SYNTAX ORA TRUE");
    stream<record {}, error?> streamData = dbClient->query("SELECT rownum, int_array, long_array, boolean_array," + 
        "string_array from ArrayTypes");

    record {} mixTypesExpected = {
        "ROWNUM": 1,
        "INT_ARRAY": [1, 2, 3],
        "LONG_ARRAY": [100000000, 200000000, 300000000],
        "BOOLEAN_ARRAY": [true, false, true],
        "STRING_ARRAY": ["Hello", "Ballerina"]
    };

    record {}? mixTypesActual = ();
    int counter = 0;
    error? e = streamData.forEach(function(record {} value) {
        if counter == 0 {
            mixTypesActual = value;
        }
        counter = counter + 1;
    });
    if e is error {
        test:assertFail("Query failed");
    }
    test:assertEquals(mixTypesActual, mixTypesExpected, "Expected record did not match.");
    test:assertEquals(counter, 4);
    check dbClient.close();
}

type ArrayRecord record {
    int row_id;
    int?[]? smallint_array;
    int?[]? int_array;
    int?[]? long_array;
    float?[]? float_array;
    float?[]? double_array;
    float?[]? real_array;
    decimal?[]? decimal_array;
    decimal?[]? numeric_array;
    string?[]? varchar_array;
    string?[]? char_array;
    string?[]? nvarchar_array;
    boolean?[]? boolean_array;
    byte[]?[]? bit_array;
    time:Date?[]? date_array;
    time:TimeOfDay?[]? time_array;
    time:Civil?[]? datetime_array;
    time:Civil?[]? timestamp_array;
    time:TimeOfDay?[]? time_tz_array;
    time:Civil?[]? timestamp_tz_array;
    byte[]?[]? blob_array;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testGetArrayTypes() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> streamData = dbClient->query(
        "SELECT row_id,smallint_array, int_array, long_array, float_array, double_array, decimal_array, real_array,numeric_array, varchar_array, char_array, nvarchar_array, boolean_array, bit_array, date_array, time_array, datetime_array, timestamp_array, blob_array, time_tz_array, timestamp_tz_array from ArrayTypes2 WHERE row_id = 1", ArrayRecord);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();
    ArrayRecord expectedData = {
        row_id: 1,
        blob_array: [<byte[]>[119, 115, 111, 50, 32, 98, 97, 108, 108, 101, 114, 105, 110, 97, 32, 98, 108, 111, 98, 32, 116, 101, 115, 116, 46], 
                    <byte[]>[119, 115, 111, 50, 32, 98, 97, 108, 108, 101, 114, 105, 110, 97, 32, 98, 108, 111, 98, 32, 116, 101, 115, 116, 46]],
        smallint_array: [12, 232],
        int_array: [1, 2, 3],
        long_array: [100000000, 200000000, 300000000],
        float_array: [245.23, 5559.49, 8796.123],
        double_array: [245.23, 5559.49, 8796.123],
        decimal_array: [245.12, 5559.12, 8796.92],
        real_array: [199.33, 2399.1],
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
        timestamp_tz_array: [<time:Civil>{utcOffset: {hours: -8, minutes: 0}, timeAbbrev: "-08:00", year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55}, <time:Civil>{utcOffset: {hours: -5, minutes: 0}, timeAbbrev: "-05:00", year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55}]
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testGetArrayTypes2() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> streamData = dbClient->query(
    "SELECT row_id,smallint_array, int_array, long_array, float_array, double_array, decimal_array, real_array,numeric_array, varchar_array, char_array, nvarchar_array, boolean_array, bit_array, date_array, time_array, datetime_array, timestamp_array, blob_array, time_tz_array, timestamp_tz_array from ArrayTypes2 WHERE row_id = 2", ArrayRecord);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
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
        varchar_array: [null, null],
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
    groups: ["query", "query-complex-params"]
}
function testGetArrayTypes3() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> streamData = dbClient->query(
        `SELECT row_id,smallint_array, int_array, long_array, float_array, double_array, decimal_array, real_array,numeric_array, varchar_array, char_array, nvarchar_array, boolean_array, bit_array, date_array, time_array, datetime_array, timestamp_array, blob_array, time_tz_array, timestamp_tz_array from ArrayTypes2 WHERE row_id = 3`, ArrayRecord);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();
    ArrayRecord expectedData = {
        row_id: 3,
        blob_array: [null, <byte[]>[119, 115, 111, 50, 32, 98, 97, 108, 108, 101, 114, 105, 110, 97, 32, 98, 108, 111, 98, 32, 116, 101, 115, 116, 46], 
                    <byte[]>[119, 115, 111, 50, 32, 98, 97, 108, 108, 101, 114, 105, 110, 97, 32, 98, 108, 111, 98, 32, 116, 101, 115, 116, 46]],
        smallint_array: [null, 12, 232],
        int_array: [null, 1, 2, 3],
        long_array: [null, 100000000, 200000000, 300000000],
        float_array: [null, 245.23, 5559.49, 8796.123],
        double_array: [null, 245.23, 5559.49, 8796.123],
        decimal_array: [null, 245.12, 5559.12, 8796.92],
        real_array: [null, 199.33, 2399.1],
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
        timestamp_tz_array: [null, <time:Civil>{utcOffset: {hours: -8, minutes: 0}, timeAbbrev: "-08:00", year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55}, <time:Civil>{utcOffset: {hours: -5, minutes: 0}, timeAbbrev: "-05:00", year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55}]
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testGetArrayTypes4() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record {}, error?> streamData = dbClient->query(
        `SELECT row_id,smallint_array, int_array, long_array, float_array, double_array, decimal_array, real_array,numeric_array, varchar_array, char_array, nvarchar_array, boolean_array, bit_array, date_array, time_array, datetime_array, timestamp_array, blob_array, time_tz_array, timestamp_tz_array from ArrayTypes2 WHERE row_id = 4`, ArrayRecord);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
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
        varchar_array: (),
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
