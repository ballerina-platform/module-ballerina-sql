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

string jdbcURL = urlPrefix + "9009/querynumericparams";

@test:BeforeGroups {
	value: ["query-numeric-params"]
} 
function initQueryNumericContainer() returns error? {
	check initializeDockerContainer("sql-query-numeric", "querynumericparams", "9009", "query", "numerical-test-data.sql");
}

@test:AfterGroups {
	value: ["query-numeric-params"]	
} 
function cleanQueryNumericContainer() returns error? {
	check cleanDockerContainer("sql-query-numeric");
}

type NumericTypeForQuery record {
    int id;
    int int_type;
    int bigint_type;
    int smallint_type;
    int tinyint_type;
    boolean bit_type;
    decimal decimal_type;
    decimal numeric_type;
    float float_type;
    float real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQuery() returns error? {
    MockClient dbClient = check new (url = jdbcURL, user = user, password = password);
    stream<record{}, Error> streamData = dbClient->query("SELECT * FROM NumericTypes");
    record {}? returnData = ();
    error? e = streamData.forEach(function(record {} data) {
        returnData = data;
    });
    check dbClient.close();

    if !(returnData is ()) {
        test:assertEquals(returnData["ID"], 1);
        test:assertEquals(returnData["INT_TYPE"], 2147483647);
        test:assertEquals(returnData["BIGINT_TYPE"], 9223372036854774807);
        test:assertEquals(returnData["SMALLINT_TYPE"], 32767);
        test:assertEquals(returnData["TINYINT_TYPE"], 127);
        test:assertEquals(returnData["BIT_TYPE"], true);
        test:assertEquals(returnData["REAL_TYPE"] is float, true);
        test:assertTrue(returnData["DECIMAL_TYPE"] is decimal);
        test:assertTrue(returnData["NUMERIC_TYPE"] is decimal);
        test:assertTrue(returnData["FLOAT_TYPE"] is float);
    } else {
        test:assertFail("Return data is nil.");
    }

}

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericTypeRecord() returns error? {
    MockClient dbClient = check new (url = jdbcURL, user = user, password = password);
    stream<record{}, error?> streamResult = dbClient->query("SELECT * FROM NumericTypes", NumericTypeForQuery);
    stream<NumericTypeForQuery, Error> streamData = <stream<NumericTypeForQuery, Error>>streamResult;
    NumericTypeForQuery? returnData = ();
    error? e = streamData.forEach(function(NumericTypeForQuery data) {
        returnData = data;
    });
    check dbClient.close();

    test:assertEquals(returnData?.id, 1);
    test:assertEquals(returnData?.int_type, 2147483647);
    test:assertEquals(returnData?.bigint_type, 9223372036854774807);
    test:assertEquals(returnData?.smallint_type, 32767);
    test:assertEquals(returnData?.tinyint_type, 127);
    test:assertEquals(returnData?.bit_type, true);
    test:assertTrue(returnData?.real_type is float);
    test:assertTrue(returnData?.decimal_type is decimal);
    test:assertTrue(returnData?.numeric_type is decimal);
    test:assertTrue(returnData?.float_type is float);
}

type NumericInvalidColumn record {|
    int num_id;
    int int_type;
    int bigint_type;
    int smallint_type;
    int tinyint_type;
    boolean bit_type;
    decimal decimal_type;
    decimal numeric_type;
    float float_type;
    float real_type;
|};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericInvalidColumnRecord() returns error? {
    MockClient dbClient = check new (url = jdbcURL, user = user, password = password);
    stream<record{}, error?> streamResult = dbClient->query("SELECT * FROM NumericTypes", NumericInvalidColumn);
    stream<NumericInvalidColumn, Error> streamData = <stream<NumericInvalidColumn, Error>> streamResult;
    record {|NumericInvalidColumn value;|}|Error? data = streamData.next();
    check streamData.close();
    check dbClient.close();
    test:assertTrue(data is error);
    error dbError = <error> data;
    test:assertEquals(dbError.message(), "No mapping field found for SQL table column 'ID' in the record type 'NumericInvalidColumn'", "Error message differs");
}

type NumericOptionalType record {
    int? id;
    int? int_type;
    int? bigint_type;
    int? smallint_type;
    int? tinyint_type;
    boolean? bit_type;
    decimal? decimal_type;
    decimal? numeric_type;
    float? float_type;
    float? real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericOptionalTypeRecord() returns error? {
    MockClient dbClient = check new (url = jdbcURL, user = user, password = password);
    stream<record{}, error?> streamResult = dbClient->query("SELECT * FROM NumericTypes", NumericOptionalType);
    stream<NumericOptionalType, Error> streamData = <stream<NumericOptionalType, Error>>streamResult;
    record {|NumericOptionalType value;|}? data = check streamData.next();
    check streamData.close();
    NumericOptionalType? returnData = data?.value;
    check dbClient.close();

    test:assertEquals(returnData?.id, 1);
    test:assertEquals(returnData?.int_type, 2147483647);
    test:assertEquals(returnData?.bigint_type, 9223372036854774807);
    test:assertEquals(returnData?.smallint_type, 32767);
    test:assertEquals(returnData?.tinyint_type, 127);
    test:assertEquals(returnData?.bit_type, true);
    test:assertTrue(returnData?.real_type is float);
    test:assertTrue(returnData?.decimal_type is decimal);
    test:assertTrue(returnData?.numeric_type is decimal);
    test:assertTrue(returnData?.float_type is float);
}

type NumericUnionType record {
    int|string id;
    int|string int_type;
    int|string bigint_type;
    int|string smallint_type;
    int|string tinyint_type;
    int|string bit_type;
    int|decimal decimal_type;
    decimal|int numeric_type;
    decimal|float? float_type;
    decimal|float? real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericUnionTypeRecord() returns error? {
    MockClient dbClient = check new (url = jdbcURL, user = user, password = password);
    stream<record{}, error?> streamResult = dbClient->query("SELECT * FROM NumericTypes", NumericUnionType);
    stream<NumericUnionType, Error> streamData = <stream<NumericUnionType, Error>>streamResult;
    record {|NumericUnionType value;|}? data = check streamData.next();
    check streamData.close();
    NumericUnionType? returnData = data?.value;
    check dbClient.close();
    
    test:assertEquals(returnData?.id, 1);
    test:assertEquals(returnData?.int_type, 2147483647);
    test:assertEquals(returnData?.bigint_type, 9223372036854774807);
    test:assertEquals(returnData?.smallint_type, 32767);
    test:assertEquals(returnData?.tinyint_type, 127);
    test:assertEquals(returnData?.bit_type, 1);
    test:assertTrue(returnData?.real_type is float);
    test:assertTrue(returnData?.decimal_type is decimal);
    test:assertTrue(returnData?.numeric_type is decimal);
    test:assertTrue(returnData?.float_type is float);
    
}

type NumericStringType record {
    string? id;
    string? int_type;
    string? bigint_type;
    string? smallint_type;
    string? tinyint_type;
    string? bit_type;
    string? decimal_type;
    string? numeric_type;
    string? float_type;
    string? real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericStringTypeRecord() returns error? {
    MockClient dbClient = check new (url = jdbcURL, user = user, password = password);
    stream<record{}, error?> streamResult = dbClient->query("SELECT * FROM NumericTypes", NumericStringType);
    stream<NumericStringType, Error> streamData = <stream<NumericStringType, Error>>streamResult;
    record {|NumericStringType value;|}? data = check streamData.next();
    check streamData.close();
    NumericStringType? returnData = data?.value;
    check dbClient.close();

    test:assertEquals(returnData?.id, "1");
    test:assertEquals(returnData?.int_type, "2147483647");
    test:assertEquals(returnData?.bigint_type, "9223372036854774807");
    test:assertEquals(returnData?.smallint_type, "32767");
    test:assertEquals(returnData?.tinyint_type, "127");
    test:assertEquals(returnData?.bit_type, "true");
    test:assertFalse(returnData?.decimal_type is ());
    test:assertFalse(returnData?.numeric_type is ());
    test:assertFalse(returnData?.float_type is ());
}

public type CustomType int|decimal|float;

type NumericCustomType record {
    CustomType id;
    CustomType int_type;
    CustomType bigint_type;
    CustomType smallint_type;
    CustomType tinyint_type;
    CustomType bit_type;
    CustomType decimal_type;
    CustomType numeric_type;
    CustomType float_type;
    CustomType real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericCustomTypeRecord() returns error? {
    MockClient dbClient = check new (url = jdbcURL, user = user, password = password);
    stream<record{}, error?> streamResult = dbClient->query("SELECT * FROM NumericTypes", NumericCustomType);
    stream<NumericCustomType, Error> streamData = <stream<NumericCustomType, Error>>streamResult;
    record {|NumericCustomType value;|}? data = check streamData.next();
    check streamData.close();
    NumericCustomType? returnData = data?.value;
    check dbClient.close();

    test:assertEquals(returnData?.id, 1);
    test:assertEquals(returnData?.int_type, 2147483647);
    test:assertEquals(returnData?.bigint_type, 9223372036854774807);
    test:assertEquals(returnData?.smallint_type, 32767);
    test:assertEquals(returnData?.tinyint_type, 127);
    test:assertEquals(returnData?.bit_type, 1);
    test:assertTrue(returnData?.real_type is float);
    test:assertTrue(returnData?.decimal_type is decimal);
    test:assertTrue(returnData?.numeric_type is decimal);
    test:assertTrue(returnData?.float_type is float);

}

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryFromNullTable() returns error? {
    MockClient dbClient = check new (url = jdbcURL, user = user, password = password);
    stream<record{}, Error> streamData = dbClient->query("SELECT * FROM NumericNullTypes");
    record {} returnData = {};
    int count = 0;
    error? e = streamData.forEach(function(record {} data) {
        returnData = data;
        count += 1;
    });
    check dbClient.close();
    test:assertEquals(count, 2, "More than one record present");
    test:assertEquals(returnData["ID"], 2);
    test:assertEquals(returnData["INT_TYPE"], ());
    test:assertEquals(returnData["BIGINT_TYPE"], ());
    test:assertEquals(returnData["SMALLINT_TYPE"], ());
    test:assertEquals(returnData["TINYINT_TYPE"], ());
    test:assertEquals(returnData["BIT_TYPE"], ());
    test:assertEquals(returnData["DECIMAL_TYPE"], ());
    test:assertEquals(returnData["NUMERIC_TYPE"], ());
    test:assertEquals(returnData["REAL_TYPE"], ());
}
