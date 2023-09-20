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

import ballerina/jballerina.java;

# Represents an SQL client.
public type Client isolated client object {

    # Executes the query, which may return multiple results.
    # When processing the stream, make sure to consume all fetched data or close the stream.
    #
    # + sqlQuery - The SQL query
    # + rowType - The `typedesc` of the record to which the result needs to be returned
    # + return - Stream of records in the `rowType` type
    remote isolated function query(ParameterizedQuery sqlQuery, typedesc<record {}> rowType = <>)
    returns stream <rowType, Error?>;

    # Executes the query, which is expected to return at most one row of the result.
    # If the query does not return any results, `sql:NoRowsError` is returned.
    #
    # + sqlQuery - The SQL query
    # + returnType - The `typedesc` of the record to which the result needs to be returned.
    #                It can be a basic type if the query result contains only one column
    # + return - Result in the `returnType` type or an `sql:Error`
    remote isolated function queryRow(ParameterizedQuery sqlQuery, typedesc<anydata> returnType = <>)
    returns returnType|Error;

    # Executes the SQL query. Only the metadata of the execution is returned (not the results from the query).
    #
    # + sqlQuery - The SQL query
    # + return - Metadata of the query execution as an `sql:ExecutionResult` or an `sql:Error`
    remote isolated function execute(ParameterizedQuery sqlQuery) returns ExecutionResult|Error;

    # Executes the SQL query with multiple sets of parameters in a batch. Only the metadata of the execution is returned (not the results from the query).
    # If one of the commands in the batch fails, an `sql:BatchExecuteError` will be returned. However, the driver may
    # or may not continue to process the remaining commands in the batch after a failure.
    #
    # + sqlQueries - The SQL query with multiple sets of parameters
    # + return - Metadata of the query execution as an `sql:ExecutionResult[]` or an `sql:Error`
    remote isolated function batchExecute(ParameterizedQuery[] sqlQueries) returns ExecutionResult[]|Error;

    # Executes an SQL query, which calls a stored procedure. This may or may not
    # return results. Once the results are processed, the `close` method on `sql:ProcedureCallResult` must be called.
    #
    # + sqlQuery - The SQL query
    # + rowTypes - `typedesc` array of the records to which the results need to be returned
    # + return - Summary of the execution and results are returned in an `sql:ProcedureCallResult`, or an `sql:Error`
    remote isolated function call(ParameterizedCallQuery sqlQuery, typedesc<record {}>[] rowTypes = [])
    returns ProcedureCallResult|Error;

    # Closes the SQL client and shuts down the connection pool. The client must be closed only at the end of the
    # application lifetime (or closed for graceful stops in a service).
    #
    # + return - Possible `sql:Error` when closing the client
    public isolated function close() returns Error?;
};

isolated function closedStreamInvocationError() returns Error {
    return error ApplicationError("Stream is closed. Therefore, no operations are allowed further on the stream.");
}

isolated function nextResult(ResultIterator iterator) returns record {}|Error? = @java:Method {
    'class: "io.ballerina.stdlib.sql.utils.RecordIteratorUtils"
} external;

isolated function closeResult(ResultIterator iterator) returns Error? = @java:Method {
    'class: "io.ballerina.stdlib.sql.utils.RecordIteratorUtils"
} external;

isolated function getNextQueryResult(ProcedureCallResult callResult) returns boolean|Error = @java:Method {
    'class: "io.ballerina.stdlib.sql.utils.ProcedureCallResultUtils"
} external;

isolated function closeCallResult(ProcedureCallResult callResult) returns Error? = @java:Method {
    'class: "io.ballerina.stdlib.sql.utils.ProcedureCallResultUtils"
} external;
