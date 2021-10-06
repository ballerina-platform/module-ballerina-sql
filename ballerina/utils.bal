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

# Concatenates all provided `sql:ParameterizedQuery`s into a single `sql:ParameterizedQuery`.
#
# + queries - Set of `sql:ParameterizedQuery` queries
# + return - An `sql:ParameterizedQuery`
public isolated function queryConcat(ParameterizedQuery... queries) returns ParameterizedQuery {
    if queries.length() == 0 {
        return ``;
    } else if queries.length() == 1 {
        return queries[0];
    } else {
        return prepareParameterizedQuery(queries);
    }
}

isolated function prepareParameterizedQuery(ParameterizedQuery[] queries) returns ParameterizedQuery {
    ParameterizedQuery newParameterizedQuery = ``;
    string[] strings = [];
    Value[] values = [];
    string previousString = "";
    foreach ParameterizedQuery query in queries {
        string[] stringValues = query.strings;
        Value[] insertionValues = query.insertions;
        int length = stringValues.length();
        if (length > 1) {
            strings.push(previousString + stringValues[0]);
            previousString = "";

            foreach var i in 1 ... length - 2 {
                strings.push(stringValues[i]);
            }
        }
        previousString = previousString + stringValues[stringValues.length() -1];
        addValues(insertionValues, values);
    }
    strings.push(previousString);
    newParameterizedQuery.insertions = values;
    newParameterizedQuery.strings = strings.cloneReadOnly();
    return newParameterizedQuery;
}

isolated function addValues(Value[] insertionValues, Value[] values) {
    foreach Value insertionValue in insertionValues {
        if insertionValue is null {
            values.push(null);
        } else {
            values.push(insertionValue);
        }
    }
}

# Joins the elements in the array with the `,` delimiter into an `sql:ParameterizedQuery`.
#
# + values - An array of `sql:Value` values
# + return - An `sql:ParameterizedQuery`
public isolated function arrayFlattenQuery(Value[] values) returns ParameterizedQuery {
    if values.length() == 0 {
        return ``;
    }
    ParameterizedQuery newParameterizedQuery = `${values[0]}`;
    foreach var i in 1..<values.length() {
        newParameterizedQuery = queryConcat(newParameterizedQuery, `, ${values[i]}`);
    }
    return newParameterizedQuery;
}

# Generates a stream consisting of `sql:Error` elements.
#
# + message - Error message used to initialise an `sql:Error`
# + return - A stream
public isolated function generateApplicationErrorStream(string message) returns stream <record {}, Error?> {
    ApplicationError applicationErr = error ApplicationError(message);
    ResultIterator resultIterator = new (err = applicationErr);
    stream<record {}, Error?> errorStream = new (resultIterator);
    return errorStream;
}
