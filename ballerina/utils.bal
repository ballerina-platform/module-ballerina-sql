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

import lang.'string;

# Concatenates all the given queries.
#
# + queries - Set of `ParameterizedQuery`
# + return - A `ParameterizedQuery`
public function queryConcat(ParameterizedQuery... queries) returns ParameterizedQuery {
    if queries.length() == 0 {
        return ``;
    } else if queries.length() == 1 {
        return queries[0];
    } else {
        return prepareParameterizedQuery(queries);
    }
}

# Flattens the given array.
#
# + values - An array of `Value`
# + return - A `ParameterizedQuery`
public function arrayFlattenQuery(Value[] values) returns ParameterizedQuery {
    ParameterizedQuery newParameterizedQuery = ``;
    string[] strings = [];
    string newQuery = "";
    foreach Value value in values {
        if value is TypedValue {
            anydata|object {}|anydata[]|object {}[]? inValues = value.value;
            if inValues is anydata[] {
                newQuery += "ARRAY[";
                string stingArray = "";
                foreach var inValue in inValues {
                    stingArray += convertToInValue(inValue);
                }
                newQuery += 'string:substring(stingArray, 0, stingArray.length() - 1) + "],";
            } else if (inValues is anydata) {
                newQuery += convertToInValue(inValues);
            }
        } else {
            newQuery += convertToInValue(value);
        }
    }
    strings[0] = 'string:substring(newQuery, 0, newQuery.length() - 1);
    newParameterizedQuery.strings = strings.cloneReadOnly();
    return newParameterizedQuery;
}

function convertToInValue(anydata value) returns string {
    if (value is null) {
        return "NULL" + ",";
    } else if (value is string) {
        return "'" + value + "',";
    } else {
        return value.toString()+ ",";
    }
}

function prepareParameterizedQuery(ParameterizedQuery[] queries) returns ParameterizedQuery {
    ParameterizedQuery newParameterizedQuery = ``;
    string queryInString = "";
    string nullValue = "";
    string[] strings = [];
    Value[] values = [];
    foreach ParameterizedQuery query in queries {
        string[] stringValues = query.strings;
        Value[] insertionValues = query.insertions;
        int i = 0;
        if insertionValues.length() == 0 {
            queryInString += stringValues[0];
        } else {
            foreach string value in stringValues {
                queryInString += value;
                strings.push(queryInString);
                if insertionValues.length() != i {
                    if insertionValues[i] is null {
                        values.push(null);
                    } else {
                        values.push(insertionValues[i]);
                    }
                    i += 1;
                }
                queryInString = nullValue;
            }
        }
    }
    if queryInString != nullValue {
        strings.push(queryInString);
    }
    newParameterizedQuery.insertions = values;
    newParameterizedQuery.strings = strings.cloneReadOnly();
    return newParameterizedQuery;
}
