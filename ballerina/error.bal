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

# Represents the properties belonging to an `sql:BatchExecuteError`.
#
# + errorCode - SQL error code
# + sqlState - SQL state
# + executionResults - Result of execution of commands
public type BatchExecuteErrorDetail record {
    int errorCode;
    string? sqlState;
    ExecutionResult[] executionResults;
};

# Represents the properties belonging to an `sql:DatabaseError`.
#
# + errorCode - SQL error code
# + sqlState - SQL state
public type DatabaseErrorDetail record {
    int errorCode;
    string? sqlState;
};

//Level 1
# Defines the common error type for the module.
public type Error distinct error;

//Level 2
# Represents an error caused by an issue related to database accessibility, erroneous queries, constraint violations,
# database resource clean-up, and other similar scenarios.
public type DatabaseError distinct Error & error<DatabaseErrorDetail>;

# Represents an error occurred when a batch execution is running.
public type BatchExecuteError distinct Error & error<BatchExecuteErrorDetail>;

# Represents an error that occurs when a query retrieves no rows when at least one row was expected.
public type NoRowsError distinct Error;

# Represents an error originating from application-level causes.
public type ApplicationError distinct Error;

//Level 3
# Represents an error during the processing of the returned data or parameters.
public type DataError distinct ApplicationError;

// Level 4
# Represents an error that occurs when a query retrieves a result that differs from the expected result type.
public type TypeMismatchError distinct DataError;

# Represents an error that occurs when a query retrieves a result that is corrupted and cannot be converted to expected type.
public type ConversionError distinct DataError;

# Represents an error that occurs when a query retrieves a result that cannot be matched to the expected record type.
public type FieldMismatchError distinct DataError;

# Represents an error that occurs when an unsupported parameter type is added to the query.
public type UnsupportedTypeError distinct DataError;
