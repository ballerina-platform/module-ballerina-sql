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

import ballerina/io;
import ballerina/time;
import ballerina/jballerina.java;

# Represents a parameter for the SQL Client remote functions when a variable needs to be passed
# to the remote function.
#
# + value - Value of parameter passed into the SQL statement
public type TypedValue object {
    public anydata|object {}? value;
};

# Possible type of parameters that can be passed into the SQL query.
public type Value string|int|boolean|float|decimal|byte[]|xml|TypedValue?;

# Represents Varchar SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class VarcharValue {
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents NVarchar SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class NVarcharValue {
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents Char SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class CharValue {
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents NChar SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class NCharValue {
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents Text SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class TextValue {
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents Clob SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class ClobValue {
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents NClob SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class NClobValue {
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents SmallInt SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class SmallIntValue {
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents Integer SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class IntegerValue {
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents BigInt SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class BigIntValue {
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents Numeric SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class NumericValue {
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents Decimal SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class DecimalValue {
    public int|decimal? value;

    public isolated function init(int|decimal? value = ()) {
        self.value = value;
    }
}

# Represents Real SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class RealValue {
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents Float SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class FloatValue {
    public int|float? value;

    public isolated function init(int|float? value = ()) {
        self.value = value;
    }
}

# Represents Double SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class DoubleValue {
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents Bit SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class BitValue {
    public boolean|int? value;

    public isolated function init(boolean|int? value = ()) {
        self.value = value;
    }
}

# Represents Boolean SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class BooleanValue {
    public boolean? value;

    public isolated function init(boolean? value = ()) {
        self.value = value;
    }
}

# Represents Binary SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class BinaryValue {
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents VarBinary SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class VarBinaryValue {
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents Blob SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class BlobValue {
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents Date SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class DateValue {
    public string|int|time:Utc? value;

    public isolated function init(string|int|time:Utc? value = ()) {
        self.value = value;
    }
}

# Represents Time SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class TimeValue {
    public string|int|time:Utc? value;

    public isolated function init(string|int|time:Utc? value = ()) {
        self.value = value;
    }
}

# Represents DateTime SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class DateTimeValue {
    public string|int|time:Utc? value;

    public isolated function init(string|int|time:Utc? value = ()) {
        self.value = value;
    }
}

# Represents Timestamp SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class TimestampValue {
    public string|int|time:Utc? value;

    public isolated function init(string|int|time:Utc? value = ()) {
        self.value = value;
    }
}

# Represents ArrayValue SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class ArrayValue {
    public string[]|int[]|boolean[]|float[]|decimal[]|byte[][]? value;

    public isolated function init(string[]|int[]|boolean[]|float[]|decimal[]|byte[][]? value = ()) {
        self.value = value;
    }
}

# Represents Ref SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class RefValue {
    public record {}? value;

    public isolated function init(record {}? value = ()) {
        self.value = value;
    }
}

# Represents Struct SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class StructValue {
    public record {}? value;

    public isolated function init(record {}? value = ()) {
        self.value = value;
    }
}

# Represents Row SQL field.
#
# + value - Value of parameter passed into the SQL statement
public class RowValue {
    public byte[]? value;

    public isolated function init(byte[]? value = ()) {
        self.value = value;
    }
}

# Represents Parameterized SQL query.
#
# + strings - The separated parts of the sql query
# + insertions - The values that should be filled in between the parts
public type ParameterizedQuery object {
    public (string[] & readonly) strings;
    public Value[] insertions;
};

# Constant indicating that the specific batch statement executed successfully
# but that no count of the number of rows it affected is available.
public const SUCCESS_NO_INFO = -2;

#Constant indicating that the specific batch statement failed.
public const EXECUTION_FAILED = -3;

# The result of the query without returning the rows.
#
# + affectedRowCount - Number of rows affected by the execution of the query. It may be one of the following,
#                      (1) A number greater than or equal to zero -- indicates that the command was processed
#                          successfully and is the affected row count in the database that were affected by
#                          the command's execution
#                      (2) A value of `SUCCESS_NO_INFO` indicates that the command was processed successfully but
#                          that the number of rows affected is unknown
#                      (3) A value of `EXECUTION_FAILED` indicated the specific command failed. This can be returned
#                          in `BatchExecuteError` and only if the driver continues to process the statements after
#                          the error occurred.
# + lastInsertId - The integer or string generated by the database in response to a query execution.
#                  Typically this will be from an "auto increment" column when inserting a new row. Not all databases
#                  support this feature, and hence it can be also nil.
public type ExecutionResult record {
    int? affectedRowCount;
    string|int? lastInsertId;
};

# Represents all OUT parameters used in SQL stored procedure call.
public type OutParameter object {

    # Parses returned Char SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error;
};

# Represents Char Out Parameter used in procedure calls
public class CharOutParameter {
    # Parses returned Char SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Varchar Out Parameter used in procedure calls
public class VarcharOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents NChar Out Parameter used in procedure calls
public class NCharOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents NVarchar Out Parameter used in procedure calls
public class NVarcharOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Binary Out Parameter used in procedure calls
public class BinaryOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents VarBinary Out Parameter used in procedure calls
public class VarBinaryOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Text Out Parameter used in procedure calls
public class TextOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Blob Out Parameter used in procedure calls
public class BlobOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Clob Out Parameter used in procedure calls
public class ClobOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents NClob Out Parameter used in procedure calls
public class NClobOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Date Out Parameter used in procedure calls
public class DateOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Time Out Parameter used in procedure calls
public class TimeOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents DateTime Out Parameter used in procedure calls
public class DateTimeOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Timestamp Out Parameter used in procedure calls
public class TimestampOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Array Out Parameter used in procedure calls
public class ArrayOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Row Out Parameter used in procedure calls
public class RowOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents SmallInt Out Parameter used in procedure calls
public class SmallIntOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Integer Out Parameter used in procedure calls
public class IntegerOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents BigInt Out Parameter used in procedure calls
public class BigIntOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Real Out Parameter used in procedure calls
public class RealOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Float Out Parameter used in procedure calls
public class FloatOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Double Out Parameter used in procedure calls
public class DoubleOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Numeric Out Parameter used in procedure calls
public class NumericOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Decimal Out Parameter used in procedure calls
public class DecimalOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Bit Out Parameter used in procedure calls
public class BitOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Boolean Out Parameter used in procedure calls
public class BooleanOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Ref Out Parameter used in procedure calls
public class RefOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Struct Out Parameter used in procedure calls
public class StructOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents XML Out Parameter used in procedure calls
public class XMLOutParameter {
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents SQL InOutParameter used in procedure calls.
public class InOutParameter {
    Value 'in;

    public isolated function init(Value 'in) {
        self.'in = 'in;
    }

    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "org.ballerinalang.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents all parameters used in SQL stored procedure call.
public type Parameter Value|InOutParameter|OutParameter;

# Represents Parameterized Call SQL Statement.
#
# + strings - The separated parts of the sql call query
# + insertions - The values that should be filled in between the parts
public type ParameterizedCallQuery object {
    public (string[] & readonly) strings;
    public Parameter[] insertions;
};

# The result iterator object that is used to iterate through the results in the event stream.
# 
# + customResultIterator - The instance of the custom ballerina class that is structurally equivalent to
#                          CustomResultIterator object type. This instance includes a custom implementation
#                          of the nextResult method. 
# + err - Used to hold any error to be returned 
# + isClosed - The boolean flag used to indicate that the result iterator is closed 
class ResultIterator {
    private boolean isClosed = false;
    private Error? err;
    public CustomResultIterator? customResultIterator;

    public isolated function init(Error? err = (), CustomResultIterator? customResultIterator = ()) {
        self.err = err;
        self.customResultIterator = customResultIterator;
    }

    public isolated function next() returns record {|record {} value;|}|Error? {
        if (self.isClosed) {
            return closedStreamInvocationError();
        }
        error? closeErrorIgnored = ();
        if (self.err is Error) {
            return self.err;
        } else {
            record {}|Error? result;
            if (self.customResultIterator is CustomResultIterator) {
                result = (<CustomResultIterator>self.customResultIterator).nextResult(self);
            }
            else {
                result = nextResult(self);
            }
            if (result is record {}) {
                record {|
                    record {} value;
                |} streamRecord = {value: result};
                return streamRecord;
            } else if (result is Error) {
                self.err = result;
                closeErrorIgnored = self.close();
                return self.err;
            } else {
                closeErrorIgnored = self.close();
                return result;
            }
        }
    }

    public isolated function close() returns Error? {
        if (!self.isClosed) {
            if (self.err is ()) {
                Error? e = closeResult(self);
                if (e is ()) {
                    self.isClosed = true;
                }
                return e;
            }
        }
    }
}

# Object that is used to return stored procedure call results.
#
# + executionResult - Summary of the execution of DML/DLL query
# + queryResult - Results from SQL query
# + customResultIterator - The instance of the custom ballerina class that is structurally equivalent to
#                          CustomResultIterator object type. This instance includes a custom implementation
#                          of the getNextQueryResult method.
public class ProcedureCallResult {
    public ExecutionResult? executionResult = ();
    public stream<record {}, Error>? queryResult = ();
    public CustomResultIterator? customResultIterator;

    public isolated function init(CustomResultIterator? customResultIterator = ()) {
        self.customResultIterator = customResultIterator;
    }

    # Updates `executionResult` or `queryResult` with the next result in the result. This will also close the current
    # results by default.
    #
    # + return - True if the next result is `queryResult`
    public isolated function getNextQueryResult() returns boolean|Error {
        if (self.customResultIterator is CustomResultIterator) {
            return (<CustomResultIterator>self.customResultIterator).getNextQueryResult(self);
        }
        return getNextQueryResult(self);
    }

    # Closes the `ProcedureCallResult` object and releases resources.
    #
    # + return - `Error` if any error occurred while closing.
    public isolated function close() returns Error? {
        return closeCallResult(self);
    }
}

# The object type that is used as a structure to define a custom class with custom
# implementations for nextResult and getNextQueryResult in the connector modules.
# 
public type CustomResultIterator object {
    isolated function nextResult(ResultIterator iterator) returns record {}|Error?;
    isolated function getNextQueryResult(ProcedureCallResult callResult) returns boolean|Error;
};
