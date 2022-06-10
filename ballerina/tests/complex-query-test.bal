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
import ballerina/io;

string complexQueryDb = urlPrefix + "9008/QueryComplexParams";

@test:BeforeGroups {
    value: ["query-complex-params"]
}
function initQueryComplexContainer() returns error? {
    check initializeDockerContainer("sql-query-complex", "QueryComplexParams", "9008", "query", "complex-test-data.sql");
}

@test:AfterGroups {
    value: ["query-complex-params"]
}
function cleanQueryComplexContainer() returns error? {
    //check cleanDockerContainer("sql-query-complex");
}

type SelectTestAlias record {
    int int_type;
    int long_type;
    float double_type;
    boolean boolean_type;
    string string_type;
};

type SelectTestAlias21 record {|
    int int_type;
    int long_type;
    float double_type;
    boolean boolean_type;
    string string_type;
    boolean active = true;
|};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testGetPrimitiveTypes() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    SelectTestAlias21 streamData = check dbClient->queryRow(
	                                                 `SELECT int_type, long_type, double_type, boolean_type, string_type
	                                                 from DataTable WHERE row_id = 1`, SelectTestAlias21);

    io:println(streamData);
}

type SelectTestAlias2 record {
    int int_type;
    int long_type;
    float double_type;
};

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

type ResultMap record {
    int[] int_array;
    int[] long_array;
    boolean[] boolean_array;
    string[] string_array;
};

type ResultDates record {
    string date_type;
    string time_type;
    string timestamp_type;
    string datetime_type;
    string time_tz_type;
    string timestamp_tz_type;
};

type ResultDates2 record {
    time:Date date_type;
    time:TimeOfDay time_type;
    time:Civil timestamp_type;
    time:Civil datetime_type;
    time:TimeOfDay time_tz_type;
    time:Civil timestamp_tz_type;
};

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

type ResultSetTestAlias record {
    int int_type;
    int long_type;
    string float_type;
    float double_type;
    boolean boolean_type;
    string string_type;
    int dt2int_type;
};

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
