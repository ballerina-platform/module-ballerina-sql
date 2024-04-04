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

# Generic type that can be passed to `sql:ParameterizedQuery` to represent parameters in the SQL query.
#
# + value - Value of the parameter
public type TypedValue object {
    public anydata|object {}|anydata[]|object {}[]? value;
};

# Represents a union type of the ballerina/time records to be used as parameter types in `sql:ParameterizedQuery`.
type DateTimeType time:Utc|time:Civil|time:Date|time:TimeOfDay|time:Civil[]|time:TimeOfDay[];

# Represents a ballerina typed array to be used as parameter types in `sql:ParameterizedQuery`.
type ArrayValueType string?[]|int?[]|boolean?[]|float?[]|decimal?[]|byte[]?[];

# Generic type of ballerina basic types that can be passed to `sql:ParameterizedQuery` to represent parameters in the SQL query.
public type Value string|int|boolean|float|decimal|byte[]|xml|record {}|DateTimeType|ArrayValueType|TypedValue?;

# Represents SQL Varchar type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class VarcharValue {
    *TypedValue;
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents SQL Varchar array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class VarcharArrayValue {
    *TypedValue;
    public string?[] value;

    public isolated function init(string?[] value = []) {
        self.value = value;
    }
}

# Represents SQL NVarchar type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class NVarcharValue {
    *TypedValue;
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents SQL NVarchar type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class NVarcharArrayValue {
    *TypedValue;
    public string?[] value;

    public isolated function init(string?[] value = []) {
        self.value = value;
    }
}

# Represents SQL Char type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class CharValue {
    *TypedValue;
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents SQL Char array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class CharArrayValue {
    *TypedValue;
    public string?[] value;

    public isolated function init(string?[] value = []) {
        self.value = value;
    }
}

# Represents SQL NChar type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class NCharValue {
    *TypedValue;
    public string? value;

    public isolated function init(string? value = ()) {
        self.value = value;
    }
}

# Represents SQL Text type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class TextValue {
    *TypedValue;
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents SQL Clob type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class ClobValue {
    *TypedValue;
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents SQL NClob type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class NClobValue {
    *TypedValue;
    public io:ReadableCharacterChannel|string? value;

    public isolated function init(io:ReadableCharacterChannel|string? value = ()) {
        self.value = value;
    }
}

# Represents SQL SmallInt type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class SmallIntValue {
    *TypedValue;
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents SQL SmallInt array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class SmallIntArrayValue {
    *TypedValue;
    public int?[] value;

    public isolated function init(int?[] value = []) {
        self.value = value;
    }
}

# Represents SQL Integer type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class IntegerValue {
    *TypedValue;
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents SQL Integer array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class IntegerArrayValue {
    *TypedValue;
    public int?[] value;

    public isolated function init(int?[] value = []) {
        self.value = value;
    }
}

# Represents SQL BigInt type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BigIntValue {
    *TypedValue;
    public int? value;

    public isolated function init(int? value = ()) {
        self.value = value;
    }
}

# Represents SQL BigInt array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BigIntArrayValue {
    *TypedValue;
    public int?[] value;

    public isolated function init(int?[] value = []) {
        self.value = value;
    }
}

# Represents SQL Numeric type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class NumericValue {
    *TypedValue;
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents SQL Numeric array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class NumericArrayValue {
    *TypedValue;
    public int?[]|float?[]|decimal?[] value;

    public isolated function init(int?[]|float?[]|decimal?[] value = <int?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Decimal type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class DecimalValue {
    *TypedValue;
    public int|decimal? value;

    public isolated function init(int|decimal? value = ()) {
        self.value = value;
    }
}

# Represents SQL Decimal array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class DecimalArrayValue {
    *TypedValue;
    public int?[]|decimal?[] value;

    public isolated function init(int?[]|decimal?[] value = <int?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Real type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class RealValue {
    *TypedValue;
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents SQL Real array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class RealArrayValue {
    *TypedValue;
    public int?[]|float?[]|decimal?[] value;

    public isolated function init(int?[]|float?[]|decimal?[] value = <int?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Float type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class FloatValue {
    *TypedValue;
    public int|float? value;

    public isolated function init(int|float? value = ()) {
        self.value = value;
    }
}

# Represents SQL Float array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class FloatArrayValue {
    *TypedValue;
    public int?[]|float?[] value;

    public isolated function init(int?[]|float?[] value = <int?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Double type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class DoubleValue {
    *TypedValue;
    public int|float|decimal? value;

    public isolated function init(int|float|decimal? value = ()) {
        self.value = value;
    }
}

# Represents SQL Double array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class DoubleArrayValue {
    *TypedValue;
    public int?[]|float?[]|decimal?[] value;

    public isolated function init(int?[]|float?[]|decimal?[] value = <int?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Bit type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BitValue {
    *TypedValue;
    public boolean|int? value;

    public isolated function init(boolean|int? value = ()) {
        self.value = value;
    }
}

# Represents SQL Bit array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BitArrayValue {
    *TypedValue;
    public boolean?[]|int?[] value;

    public isolated function init(boolean?[]|int?[] value = <int?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Boolean type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BooleanValue {
    *TypedValue;
    public boolean? value;

    public isolated function init(boolean? value = ()) {
        self.value = value;
    }
}

# Represents SQL Boolean array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BooleanArrayValue {
    *TypedValue;
    public boolean?[] value;

    public isolated function init(boolean?[] value = []) {
        self.value = value;
    }
}

# Represents SQL Binary type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BinaryValue {
    *TypedValue;
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents SQL Boolean array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BinaryArrayValue {
    *TypedValue;
    public byte[]?[]|io:ReadableByteChannel[] value;

    public isolated function init(io:ReadableByteChannel[]|byte[]?[] value = <byte[]?[]>[]) {
        self.value = value;
    }
}

# Represents SQL VarBinary type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class VarBinaryValue {
    *TypedValue;
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents SQL Boolean array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class VarBinaryArrayValue {
    *TypedValue;
    public byte[]?[]|io:ReadableByteChannel[] value;

    public isolated function init(byte[]?[]|io:ReadableByteChannel[] value = <byte[]?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Blob type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class BlobValue {
    *TypedValue;
    public byte[]|io:ReadableByteChannel? value;

    public isolated function init(byte[]|io:ReadableByteChannel? value = ()) {
        self.value = value;
    }
}

# Represents SQL Date type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class DateValue {
    *TypedValue;
    public string|time:Date? value;

    public isolated function init(string|time:Date? value = ()) {
        self.value = value;
    }
}

# Represents SQL Date array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class DateArrayValue {
    *TypedValue;
    public string?[]|time:Date?[] value;

    public isolated function init(string?[]|time:Date?[] value = <string?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Time type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class TimeValue {
    *TypedValue;
    public string|time:TimeOfDay? value;

    public isolated function init(string|time:TimeOfDay? value = ()) {
        self.value = value;
    }
}

# Represents SQL Time array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class TimeArrayValue {
    *TypedValue;
    public string?[]|time:TimeOfDay?[] value;

    public isolated function init(string?[]|time:TimeOfDay?[] value = <string?[]>[]) {
        self.value = value;
    }
}

# Represents SQL DateTime type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class DateTimeValue {
    *TypedValue;
    public string|time:Civil? value;

    public isolated function init(string|time:Civil? value = ()) {
        self.value = value;
    }
}

# Represents SQL DateTime array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class DateTimeArrayValue {
    *TypedValue;
    public string?[]|time:Civil?[] value;

    public isolated function init(string?[]|time:Civil?[] value = <string?[]>[]) {
        self.value = value;
    }
}

# Represents SQL Timestamp type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class TimestampValue {
    *TypedValue;
    public string|time:Utc? value;

    public isolated function init(string|time:Utc? value = ()) {
        self.value = value;
    }
}

# Represents SQL Timestamp array type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class TimestampArrayValue {
    *TypedValue;
    public string?[]|time:Utc?[] value;

    public isolated function init(string?[]|time:Utc?[] value = <string?[]>[]) {
        self.value = value;
    }
}

# Represents SQL ArrayValue type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
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

# Represents SQL Ref type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class RefValue {
    *TypedValue;
    public record {}? value;

    public isolated function init(record {}? value = ()) {
        self.value = value;
    }
}

# Represents SQL Struct type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class StructValue {
    *TypedValue;
    public record {}? value;

    public isolated function init(record {}? value = ()) {
        self.value = value;
    }
}

# Represents SQL Row type parameter in `sql:ParameterizedQuery`.
#
# + value - Value of the parameter
public distinct class RowValue {
    *TypedValue;
    public byte[]? value;

    public isolated function init(byte[]? value = ()) {
        self.value = value;
    }
}

# The object constructed through backtick surrounded strings. Dynamic parameters of `sql:Value` type can be indicated using `${<variable name>}`
# such as `` `The sql:ParameterizedQuery is ${variable_name}` ``.
# This validates the parameter types during the query execution.  
#
# + strings - The separated parts of the SQL query 
# + insertions - The values of the parameters that should be filled in between the parts
public type ParameterizedQuery distinct object {
    *obj:RawTemplate;
    public (string[] & readonly) strings;
    public Value[] insertions;
};

# Constant indicating that the specific batch statement executed successfully
# but that the count of affected rows is unavailable.
public const SUCCESS_NO_INFO = -2;

# Constant indicating that the specific batch statement failed.
public const EXECUTION_FAILED = -3;

# Metadata of the query execution.
#
# + affectedRowCount - Number of rows affected by the execution of the query. It may be one of the following,  
#                      (1) A number greater than or equal to zero, the count of affected rows after the successful execution of the query  
#                      (2) A value of the `SUCCESS_NO_INFO`, the count of affected rows is unknown after the successful execution of the query  
#                      (3) A value of the `EXECUTION_FAILED`, the query execution failed
# + lastInsertId - The ID generated by the database in response to a query execution. This can be `()` in case the database does not support this feature
public type ExecutionResult record {
    int? affectedRowCount;
    string|int? lastInsertId;
};

# Represents the generic OUT Parameters in `sql:ParameterizedCallQuery`.
public type OutParameter object {

    # Parses returned Char SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error;
};

# Represents Char Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class CharOutParameter {
    *OutParameter;
    # Parses returned Char SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Char Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class CharArrayOutParameter {
    *OutParameter;
    # Parses returned Char SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Varchar Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class VarcharOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Varchar Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class VarcharArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents NChar Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class NCharOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents NVarchar Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class NVarcharOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents NVarchar Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class NVarcharArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Binary Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BinaryOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Binary Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BinaryArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents VarBinary Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class VarBinaryOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents VarBinary Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class VarBinaryArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Text Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TextOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Blob Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BlobOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Clob Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class ClobOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents NClob Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class NClobOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Date Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class DateOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Date Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class DateArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Time Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TimeOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Time Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TimeArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Time With Timezone Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TimeWithTimezoneOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Time With Timezone Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TimeWithTimezoneArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents DateTime Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class DateTimeOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents DateTime Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class DateTimeArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Timestamp Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TimestampOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Timestamp Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TimestampArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Timestamp with Timezone Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TimestampWithTimezoneOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Timestamp with Timezone Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class TimestampWithTimezoneArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class ArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Row Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class RowOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents SmallInt Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class SmallIntOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents SmallInt Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class SmallIntArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Integer Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class IntegerOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents BigInt Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BigIntOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Integer Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class IntegerArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents BigInt Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BigIntArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Real Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class RealOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Real Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class RealArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Float Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class FloatOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Float Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class FloatArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Double Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class DoubleOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Double Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class DoubleArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Numeric Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class NumericOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Numeric Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class NumericArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Decimal Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class DecimalOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Decimal Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class DecimalArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Bit Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BitOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Bit Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BitArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Boolean Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BooleanOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Boolean Array Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class BooleanArrayOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Ref Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class RefOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents Struct Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class StructOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents XML Out Parameter in `sql:ParameterizedCallQuery`.
public distinct class XMLOutParameter {
    *OutParameter;
    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutParameterValue"
    } external;
}

# Represents the Cursor Out Parameters in `sql:ParameterizedCallQuery`.
public class CursorOutParameter {

    # Parses returned SQL result set values to a ballerina stream value.
    #
    # + rowType - The `typedesc` of the record to which the result needs to be returned
    # + return - Stream of records in the `rowType` type
    public isolated function get(typedesc<record {}> rowType = <>) returns stream <rowType, Error?> = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutCursorValue"
    } external;
};

# Represents SQL InOutParameter in `sql:ParameterizedCallQuery`.
public class InOutParameter {
    Value 'in;

    public isolated function init(Value 'in) {
        self.'in = 'in;
    }

    # Parses returned SQL value to a ballerina value.
    #
    # + typeDesc - The `typedesc` of the type to which the result needs to be returned
    # + return - The result in the `typeDesc` type, or an `sql:Error`
    public isolated function get(typedesc<anydata> typeDesc = <>) returns typeDesc|Error = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getInOutParameterValue"
    } external;
}

# Represents a Cursor InOut Parameter in `sql:ParameterizedCallQuery`.
public class CursorInOutParameter {
    Value 'in;

    // value should be stream<record{}, sql:error>
    public isolated function init() {
        self.'in = ();
    }

    # Gets the cursor value.
    #
    # + rowType - The `typedesc` of the record to which the result needs to be returned
    # + return - The cursor stream of records in the `rowType` type
    public isolated function get(typedesc<record {}> rowType = <>) returns stream <rowType, Error?> = @java:Method {
        'class: "io.ballerina.stdlib.sql.nativeimpl.OutParameterProcessor",
        name: "getOutCursorValue"
    } external;
}

# Generic type that can be passed to `sql:ParameterizedCallQuery` to indicate procedure/function parameters.
public type Parameter Value|InOutParameter|OutParameter|CursorOutParameter|CursorInOutParameter;

# The object constructed through backtick surrounded strings. Dynamic parameters of `sql:Parameter` type can be indicated using `${<variable name>}`
# such as `` `The sql:ParameterizedQuery is ${variable_name}` ``.
# This validates the parameter types during the query execution.
#
# + strings - The separated parts of the SQL query 
# + insertions - The values of the parameters that should be filled in between the parts
public type ParameterizedCallQuery distinct object {
    *obj:RawTemplate;
    public (string[] & readonly) strings;
    public Parameter[] insertions;
};

# The result iterator used to iterate results in stream returned from `query` function.
#
# + customResultIterator - Any custom result iterator to be used to override the default behaviour
# + err - Used to hold any error that occurs at the instance of the stream creation
# + isClosed - Indicates the stream state
public class ResultIterator {
    private boolean isClosed = false;
    private Error? err;
    public CustomResultIterator? customResultIterator;

    public isolated function init(Error? err = (), CustomResultIterator? customResultIterator = ()) {
        self.err = err;
        self.customResultIterator = customResultIterator;
    }

    public isolated function next() returns record {|record {} value;|}|Error? {
        if self.isClosed {
            return closedStreamInvocationError();
        }
        if self.err is Error {
            return self.err;
        } else {
            record {}|Error? result;
            if self.customResultIterator is CustomResultIterator {
                result = (<CustomResultIterator>self.customResultIterator).nextResult(self);
            } else {
                result = nextResult(self);
            }
            if result is record {} {
                record {|
                    record {} value;
                |} streamRecord = {value: result};
                return streamRecord;
            } else if result is Error {
                self.err = result;
                self.isClosed = true;
                return self.err;
            } else {
                self.isClosed = true;
                return result;
            }
        }
    }

    public isolated function close() returns Error? {
        if !self.isClosed {
            if self.err is () {
                Error? e = closeResult(self);
                if e is () {
                    self.isClosed = true;
                }
                return e;
            }
        }
    }
}

# Represents the results from the `call` method holding the returned results or metadata of the query execution.
#
# + executionResult - Summary of the query execution
# + queryResult - Results from the SQL query
# + customResultIterator - Any custom result iterator to be used to override the default behaviour
public class ProcedureCallResult {
    public ExecutionResult? executionResult = ();
    public stream<record {}, Error?>? queryResult = ();
    public CustomResultIterator? customResultIterator;

    public isolated function init(CustomResultIterator? customResultIterator = ()) {
        self.customResultIterator = customResultIterator;
    }

    # Updates `executionResult` or `queryResult` field with the succeeding result in the result list. This will also close the current
    # result when called. The `close` method must be called once all the results are processed.
    #
    # + return - True if the next result is `queryResult`
    public isolated function getNextQueryResult() returns boolean|Error {
        if self.customResultIterator is CustomResultIterator {
            return (<CustomResultIterator>self.customResultIterator).getNextQueryResult(self);
        }
        return getNextQueryResult(self);
    }

    # Releases the associated resources such as the database connection, results, etc.
    # This method must be called once all results are processed.
    #
    # + return - An `sql:Error` if any error occurred while cleanup
    public isolated function close() returns Error? {
        return closeCallResult(self);
    }
}

# The iterator for the stream returned in the `query` function to be used to override the default behavior of the `sql:ResultIterator`.
public type CustomResultIterator object {
    public isolated function nextResult(ResultIterator iterator) returns record {}|Error?;
    public isolated function getNextQueryResult(ProcedureCallResult callResult) returns boolean|Error;
};
