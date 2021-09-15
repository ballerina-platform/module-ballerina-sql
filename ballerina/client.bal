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

# Represents a SQL client.
#
public type Client client object {

    # Queries the database with the query provided by the user, and returns the result as a stream.
    #
    # + sqlQuery - The query, which needs to be executed as an `sql:ParameterizedQuery`. Usage of `string` is depreciated
    # + rowType - The `typedesc` of the record that should be returned as a result. If this is not provided, the default
    #             column names of the query result set will be used for the record attributes
    # + return - Stream of records in the type of `rowType`
    remote isolated function query(string|ParameterizedQuery sqlQuery, typedesc<record {}> rowType = <>)
    returns stream <rowType, Error?>;

    # Queries the database with the provided query and returns the first row as a record if the expected return type is
    # a record. If the expected return type is not a record, then a single value is returned.
    #
    # + sqlQuery - The query to be executed as an `sql:ParameterizedQuery`, which returns only one result row
    # + returnType - The `typedesc` of the record/type that should be returned as a result. If this is not provided, the
    #                default column names/type of the query result set will be used
    # + return - Result in the `returnType` type
    remote isolated function queryRow(ParameterizedQuery sqlQuery, typedesc<any> returnType = <>)
    returns returnType|Error;

    # Executes the provided DDL or DML SQL query and returns a summary of the execution.
    #
    # + sqlQuery - The DDL or DML queries such as `INSERT`, `DELETE`, `UPDATE`, etc. as an `sql:ParameterizedQuery`.
    #              Usage of `string` is depreciated
    # + return - Summary of the SQL update query as an `sql:ExecutionResult` or an `sql:Error`
    #            if any error occurred when executing the query
    remote isolated function execute(string|ParameterizedQuery sqlQuery) returns ExecutionResult|Error;

    # Executes a provided batch of parameterized DDL or DML SQL queries
    # and returns the summary of the execution.
    #
    # + sqlQueries - The DDL or DML queries such as `INSERT`, `DELETE`, `UPDATE`, etc. as an `sql:ParameterizedQuery`
    #                with an array of values passed in
    # + return - Summary of the executed SQL queries as an `sql:ExecutionResult[]`, which includes details such as
    #            `affectedRowCount` and `lastInsertId`. If one of the commands in the batch fails, this function
    #            will return an `sql:BatchExecuteError`. However, the driver may or may not continue to process the
    #            remaining commands in the batch after a failure. The summary of the executed queries in case of an error
    #            can be accessed as `(<sql:BatchExecuteError> result).detail()?.executionResults`
    remote isolated function batchExecute(ParameterizedQuery[] sqlQueries) returns ExecutionResult[]|Error;

    # Executes a SQL stored procedure and returns the result as stream and execution summary.
    #
    # + sqlQuery - The query to execute the SQL stored procedure as an `sql:ParameterizedQuery`. Usage of `string` is depreciated
    # + rowTypes - The array of `typedesc` of the records that should be returned as a result. If this is not provided,
    #               the default column names of the query result set will be used for the record attributes
    # + return - Summary of the execution is returned in an `sql:ProcedureCallResult`, or an `sql:Error`
    remote isolated function call(string|ParameterizedCallQuery sqlQuery, typedesc<record {}>[] rowTypes = [])
    returns ProcedureCallResult|Error;

    # Closes the SQL client.
    #
    # + return - Possible error when closing the client
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
