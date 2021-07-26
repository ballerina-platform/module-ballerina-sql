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
import ballerina/lang.'object as obj;

# Represents a parameter for the SQL Client remote functions when a variable needs to be passed.
# to the remote function.
#
# + value - Value of parameter passed into the SQL statement
public type TypedValue object {
    public anydata|object {}|anydata[]|object{}[]? value;
};

# Represents a union type of ballerina/time records.
type DateTimeType time:Utc|time:Civil|time:Date|time:TimeOfDay|time:Civil[]|time:TimeOfDay[];

# Represents ballerina typed array.
type ArrayValueType string?[]|int?[]|boolean?[]|float?[]|decimal?[]|byte[]?[];

# Possible type of parameters that can be passed into the SQL query.
public type Value string|int|boolean|float|decimal|byte[]|xml|DateTimeType|ArrayValueType|TypedValue?;

# Represents Varchar SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class VarcharValue {
    *TypedValue;
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents Varchar array  SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class VarcharArrayValue {
    *TypedValue;
    public string?[] value;

    public isolated function init(string?[] value = []) {
        self.value = value;
    }
}

# Represents NVarchar SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class NVarcharValue {
    *TypedValue;
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents Varchar NVarchar  SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class NVarcharArrayValue {
    *TypedValue;
    public string?[] value;

    public isolated function init(string?[] value = []) {
        self.value = value;
    }
}

# Represents Char SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class CharValue {
    *TypedValue;
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents Char array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class CharArrayValue {
    *TypedValue;
    public string?[] value;

    public isolated function init(string?[] value = []) {
        self.value = value;
    }
}

# Represents NChar SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class NCharValue {
    *TypedValue;
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents Text SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class TextValue {
    *TypedValue;
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents Clob SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class ClobValue {
    *TypedValue;
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents NClob SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class NClobValue {
    *TypedValue;
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents SmallInt SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class SmallIntValue {
    *TypedValue;
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents SmallInt array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class SmallIntArrayValue {
    *TypedValue;
    public int?[] value;

    public isolated function init(int?[] value = []) {
        self.value = value;
    }
}

# Represents Integer SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class IntegerValue {
    *TypedValue;
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents Integer array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class IntegerArrayValue {
    *TypedValue;
    public int?[] value;

    public isolated function init(int?[] value = []) {
        self.value = value;
    }
}

# Represents BigInt SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BigIntValue {
    *TypedValue;
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents BigInt array  SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BigIntArrayValue {
    *TypedValue;
    public int?[] value;

    public isolated function init(int?[] value = []) {
        self.value = value;
    }
}

# Represents Numeric SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class NumericValue {
    *TypedValue;
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents Numeric array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class NumericArrayValue {
    *TypedValue;
    public int?[]|float?[]|decimal?[] value;

    public isolated function init(int?[]|float?[]|decimal?[] value = <int?[]> []) {
        self.value = value;
    }
}

# Represents Decimal SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class DecimalValue {
    *TypedValue;
    public int|decimal? value;

    public isolated function init(int|decimal? value = ()) {
        self.value = value;
    }
}

# Represents Decimal array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class DecimalArrayValue {
    *TypedValue;
    public int?[]|decimal?[] value;

    public isolated function init(int?[]|decimal?[] value = <int?[]> []) {
        self.value = value;
    }
}

# Represents Real SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class RealValue {
    *TypedValue;
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents Real array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class RealArrayValue {
    *TypedValue;
    public int?[]|float?[]|decimal?[] value;

    public isolated function init(int?[]|float?[]|decimal?[] value = <int?[]> []) {
        self.value = value;
    }
}

# Represents Float SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class FloatValue {
    *TypedValue;
    public int|float? value;

    public isolated function init(int|float? value = ()) {
        self.value = value;
    }
}

# Represents Float array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class FloatArrayValue {
    *TypedValue;
    public int?[]|float?[] value;

    public isolated function init(int?[]|float?[] value = <int?[]> []) {
        self.value = value;
    }
}

# Represents Double SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class DoubleValue {
    *TypedValue;
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents Double array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class DoubleArrayValue {
    *TypedValue;
    public int?[]|float?[]|decimal?[] value;

    public isolated function init(int?[]|float?[]|decimal?[] value = <int?[]> []) {
        self.value = value;
    }
}

# Represents Bit SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BitValue {
    *TypedValue;
    public boolean|int? value;

    public isolated function init(boolean|int? value = ()) {
        self.value = value;
    }
}

# Represents Bit array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BitArrayValue {
    *TypedValue;
    public boolean?[]|int?[] value;

    public isolated function init(boolean?[]|int?[] value = <int?[]> []) {
        self.value = value;
    }
}

# Represents Boolean SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BooleanValue {
    *TypedValue;
    public boolean? value;

    public isolated function init(boolean? value = ()) {
        self.value = value;
    }
}

# Represents Boolean array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BooleanArrayValue {
    *TypedValue;
    public boolean?[] value;

    public isolated function init(boolean?[] value = []) {
        self.value = value;
    }
}

# Represents Binary SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BinaryValue {
    *TypedValue;
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents Boolean array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BinaryArrayValue {
    *TypedValue;
    public byte[]?[]|io:ReadableByteChannel[] value;

    public isolated function init(io:ReadableByteChannel[]|byte[]?[] value = <byte[]?[]>[]) {
        self.value = value;
    }
}

# Represents VarBinary SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class VarBinaryValue {
    *TypedValue;
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents Boolean array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class VarBinaryArrayValue {
    *TypedValue;
    public byte[]?[]|io:ReadableByteChannel[] value;

    public isolated function init(byte[]?[]|io:ReadableByteChannel[] value = <byte[]?[]>[]) {
        self.value = value;
    }
}

# Represents Blob SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class BlobValue {
    *TypedValue;
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents Date SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class DateValue {
    *TypedValue;
    public string|time:Date? value;

    public isolated function init(string|time:Date? value = ()) {
        self.value = value;
    }
}

# Represents Date array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class DateArrayValue {
    *TypedValue;
    public string?[]|time:Date?[] value;

    public isolated function init(string?[]|time:Date?[] value = <string?[]> []) {
        self.value = value;
    }
}

# Represents Time SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class TimeValue {
    *TypedValue;
    public string|time:TimeOfDay? value;

    public isolated function init(string|time:TimeOfDay? value = ()) {
        self.value = value;
    }
}

# Represents Time array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class TimeArrayValue {
    *TypedValue;
    public string?[]|time:TimeOfDay?[] value;

    public isolated function init(string?[]|time:TimeOfDay?[] value = <string?[]> []) {
        self.value = value;
    }
}

# Represents DateTime SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class DateTimeValue {
    *TypedValue;
    public string|time:Civil? value;

    public isolated function init(string|time:Civil? value = ()) {
        self.value = value;
    }
}

# Represents DateTime array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class DateTimeArrayValue {
    *TypedValue;
    public string?[]|time:Civil?[] value;

    public isolated function init(string?[]|time:Civil?[] value = <string?[]> []) {
        self.value = value;
    }
}

# Represents Timestamp SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class TimestampValue {
    *TypedValue;
    public string|time:Utc? value;

    public isolated function init(string|time:Utc? value = ()) {
        self.value = value;
    }
}

# Represents Timestamp array SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class TimestampArrayValue {
    *TypedValue;
    public string?[]|time:Utc?[] value;

    public isolated function init(string?[]|time:Utc?[] value = <string?[]> []) {
        self.value = value;
    }
}

# Represents ArrayValue SQL field.
#
# + value - Value of parameter passed into the SQL statement
#
# # Deprecated
# This `ArrayValue` class deprecated by introducing the a new `ArrayValueType` type.
@deprecated
public distinct class ArrayValue {
    *TypedValue;
    public string[]|int[]|boolean[]|float[]|decimal[]|byte[][]? value;

    @deprecated
    public isolated function init(string[]|int[]|boolean[]|float[]|decimal[]|byte[][]? value = ()) {
        self.value = value;
    }
}

# Represents Ref SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class RefValue {
    *TypedValue;
    public record {}? value;

    public isolated function init(record {}? value = ()) {
        self.value = value;
    }
}

# Represents Struct SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class StructValue {
    *TypedValue;
    public record {}? value;

    public isolated function init(record {}? value = ()) {
        self.value = value;
    }
}

# Represents Row SQL field.
#
# + value - Value of parameter passed into the SQL statement
public distinct class RowValue {
    *TypedValue;
    public byte[]? value;

    public isolated function init(byte[]? value = ()) {
        self.value = value;
    }
}

# Represents Parameterized SQL query.
#
# + strings - The separated parts of the sql query
# + insertions - The values that should be filled in between the parts
public type ParameterizedQuery distinct object {
    *obj:RawTemplate;
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
#                      (2) A value of the `SUCCESS_NO_INFO` indicates that the command was processed successfully but
#                          that the number of rows affected is unknown
#                      (3) A value of the `EXECUTION_FAILED` indicated the specific command that failed. This can be
#                          returned in `sql:BatchExecuteError` and only if the driver continues to process the
#                          statements after the error occurred
# + lastInsertId - The integer or string generated by the database in response to a query execution.
#                  Typically this will be from an `auto increment` column when inserting a new row. Not all databases
#                  support this feature, and thereby, it can also be `()`
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

# Represents Char Out Parameter used in procedure calls.
public distinct class CharOutParameter {
    *OutParameter;
    # Parses returned Char SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Char Array Out Parameter used in procedure calls
public distinct class CharArrayOutParameter {
    *OutParameter;
    # Parses returned Char SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Varchar Out Parameter used in procedure calls.
public distinct class VarcharOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Varchar Array Out Parameter used in procedure calls.
public distinct class VarcharArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents NChar Out Parameter used in procedure calls.
public distinct class NCharOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents NVarchar Out Parameter used in procedure calls.
public distinct class NVarcharOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents NVarchar Array Out Parameter used in procedure calls.
public distinct class NVarcharArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Binary Out Parameter used in procedure calls.
public distinct class BinaryOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Binary Array Out Parameter used in procedure calls.
public distinct class BinaryArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents VarBinary Out Parameter used in procedure calls.
public distinct class VarBinaryOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents VarBinary Array Out Parameter used in procedure calls.
public distinct class VarBinaryArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Text Out Parameter used in procedure calls.
public distinct class TextOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Blob Out Parameter used in procedure calls.
public distinct class BlobOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Clob Out Parameter used in procedure calls.
public distinct class ClobOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents NClob Out Parameter used in procedure calls.
public distinct class NClobOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Date Out Parameter used in procedure calls.
public distinct class DateOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Date Array Out Parameter used in procedure calls.
public distinct class DateArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Time Out Parameter used in procedure calls.
public distinct class TimeOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Time Array Out Parameter used in procedure calls.
public distinct class TimeArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Time With Timezone Out Parameter used in procedure calls.
public distinct class TimeWithTimezoneOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Time With Timezone Array Out Parameter used in procedure calls.
public distinct class TimeWithTimezoneArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents DateTime Out Parameter used in procedure calls.
public distinct class DateTimeOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents DateTime Array Out Parameter used in procedure calls.
public distinct class DateTimeArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Timestamp Out Parameter used in procedure calls.
public distinct class TimestampOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Timestamp Array Out Parameter used in procedure calls.
public distinct class TimestampArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Timestamp with Timezone Out Parameter used in procedure calls.
public distinct class TimestampWithTimezoneOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Timestamp with Timezone Array Out Parameter used in procedure calls.
public distinct class TimestampWithTimezoneArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Array Out Parameter used in procedure calls.
public distinct class ArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Row Out Parameter used in procedure calls.
public distinct class RowOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents SmallInt Out Parameter used in procedure calls.
public distinct class SmallIntOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents SmallInt Array Out Parameter used in procedure calls.
public distinct class SmallIntArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Integer Out Parameter used in procedure calls.
public distinct class IntegerOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents BigInt Out Parameter used in procedure calls.
public distinct class BigIntOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Integer Array Out Parameter used in procedure calls.
public distinct class IntegerArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents BigInt Array Out Parameter used in procedure calls
public distinct class BigIntArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Real Out Parameter used in procedure calls.
public distinct class RealOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Real Array Out Parameter used in procedure calls.
public distinct class RealArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Float Out Parameter used in procedure calls.
public distinct class FloatOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Float Array Out Parameter used in procedure calls.
public distinct class FloatArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Double Out Parameter used in procedure calls.
public distinct class DoubleOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Double Array Out Parameter used in procedure calls.
public distinct class DoubleArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Numeric Out Parameter used in procedure calls.
public distinct class NumericOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Numeric Array Out Parameter used in procedure calls.
public distinct class NumericArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Decimal Out Parameter used in procedure calls.
public distinct class DecimalOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Decimal Out Parameter used in procedure calls.
public distinct class DecimalArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Bit Out Parameter used in procedure calls.
public distinct class BitOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Bit Array Out Parameter used in procedure calls.
public distinct class BitArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Boolean Out Parameter used in procedure calls.
public distinct class BooleanOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Boolean Array Out Parameter used in procedure calls.
public distinct class BooleanArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Ref Out Parameter used in procedure calls.
public distinct class RefOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents Struct Out Parameter used in procedure calls.
public distinct class StructOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents XML Out Parameter used in procedure calls.
public distinct class XMLOutParameter {
    *OutParameter;
    # Parses returned SQL value to ballerina value.
    #
    # + typeDesc - Type description of the data that need to be converted
    # + return - The converted ballerina value or Error
    public isolated function get(typedesc<anydata> typeDesc) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
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
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor"
    } external;
}

# Represents all parameters used in SQL stored procedure call.
public type Parameter Value|InOutParameter|OutParameter;

# Represents Parameterized Call SQL Statement.
#
# + strings - The separated parts of the sql call query
# + insertions - The values that should be filled in between the parts
public type ParameterizedCallQuery distinct object {
    *obj:RawTemplate;
    public (string[] & readonly) strings;
    public Parameter[] insertions;
};

# The result iterator object that is used to iterate through the results in the event stream.
# 
# + customResultIterator - The instance of the custom Ballerina class that is structurally equivalent to
#                          the `customResultIterator` object type. This instance includes a custom implementation
#                          of the `nextResult` method
# + err - Used to hold any error to be returned 
# + isClosed - The boolean flag used to indicate that the result iterator is closed 
public class ResultIterator {
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
# + queryResult - Results from the SQL query
# + customResultIterator - The instance of the custom Ballerina class that is structurally equivalent to
#                          the `customResultIterator` object type. This instance includes a custom implementation
#                          of the `getNextQueryResult` method
public class ProcedureCallResult {
    public ExecutionResult? executionResult = ();
    public stream<record {}, Error?>? queryResult = ();
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

    # Closes the `sql:ProcedureCallResult` object and releases the associated resources.
    #
    # + return - An `sql:Error` if any error occurred while closing
    public isolated function close() returns Error? {
        return closeCallResult(self);
    }
}

# The object type that is used as a structure to define a custom class with custom
# implementations for nextResult and getNextQueryResult in the connector modules.
# 
public type CustomResultIterator object {
    public isolated function nextResult(ResultIterator iterator) returns record {}|Error?;
    public isolated function getNextQueryResult(ProcedureCallResult callResult) returns boolean|Error;
};
