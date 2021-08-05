/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.sql;

import io.ballerina.runtime.api.values.BString;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;

/**
 * Constants for SQL client.
 *
 * @since 1.2.0
 */
public final class Constants {
    private Constants() {}

    public static final String CONNECTOR_NAME = "SQLClientConnector";
    public static final String DATABASE_CLIENT = "Client";
    public static final String DATABASE_CLIENT_ACTIVE_STATUS = "clientActive";
    public static final String SQL_CONNECTOR_TRANSACTION_ID = "sql-transaction-id";


    public static final String BATCH_EXECUTE_ERROR_DETAIL = "BatchExecuteErrorDetail";
    public static final String BATCH_EXECUTE_ERROR = "BatchExecuteError";
    public static final String BATCH_EXECUTE_ERROR_MESSAGE = "Error occurred when batch executing commands.";
    public static final String DATABASE_ERROR_DETAILS = "DatabaseErrorDetail";
    public static final String DATABASE_ERROR = "DatabaseError";
    public static final String APPLICATION_ERROR = "ApplicationError";
    public static final String NO_ROWS_ERROR = "NoRowsError";
    public static final String TYPE_MISMATCH_ERROR = "TypeMismatchError";
    public static final String DATABASE_ERROR_MESSAGE = "Database Error Occurred";

    public static final String RESULT_ITERATOR_OBJECT = "ResultIterator";
    public static final String RESULT_SET_NATIVE_DATA_FIELD = "ResultSet";
    public static final String CONNECTION_NATIVE_DATA_FIELD = "Connection";
    public static final String STATEMENT_NATIVE_DATA_FIELD = "Statement";
    public static final String COLUMN_DEFINITIONS_DATA_FIELD = "ColumnDefinition";
    public static final String RECORD_TYPE_DATA_FIELD = "recordType";

    public static final String PROCEDURE_CALL_RESULT = "ProcedureCallResult";
    public static final String TYPE_DESCRIPTIONS_NATIVE_DATA_FIELD = "TypeDescription";
    public static final String RESULT_SET_COUNT_NATIVE_DATA_FIELD = "ResultSetCount";
    public static final String RESULT_SET_TOTAL_NATIVE_DATA_FIELD = "ResultSetTotal";
    public static final BString EXECUTION_RESULT_FIELD = fromString("executionResult");
    public static final BString QUERY_RESULT_FIELD = fromString("queryResult");

    public static final BString TIMEZONE_UTC = fromString("UTC");

    public static final String EXECUTION_RESULT_RECORD = "ExecutionResult";
    public static final String AFFECTED_ROW_COUNT_FIELD = "affectedRowCount";
    public static final String LAST_INSERTED_ID_FIELD = "lastInsertId";

    public static final String READ_BYTE_CHANNEL_STRUCT = "ReadableByteChannel";
    public static final String READ_CHAR_CHANNEL_STRUCT = "ReadableCharacterChannel";

    public static final String USERNAME = "user";
    public static final String PASSWORD = "password";

    /**
     * Constants related connection pool.
     */
    public static final class ConnectionPool {
        private ConnectionPool() {}
        public static final BString MAX_OPEN_CONNECTIONS = fromString("maxOpenConnections");
        public static final BString MAX_CONNECTION_LIFE_TIME = fromString(
                "maxConnectionLifeTime");
        public static final BString MIN_IDLE_CONNECTIONS = fromString("minIdleConnections");
    }

    /**
     * Constants related to database options.
     */
    public static final class Options {
        private Options() {}
        public static final BString URL = fromString("url");
    }

    /**
     * Constant related error fields.
     */
    public static final class ErrorRecordFields {
        private ErrorRecordFields() {}
        public static final String ERROR_CODE = "errorCode";
        public static final String SQL_STATE = "sqlState";
        public static final String EXECUTION_RESULTS = "executionResults";

    }

    /**
     * Constants related to parameterized string fields.
     */
    public static final class ParameterizedQueryFields {
        private ParameterizedQueryFields() {}
        public static final BString STRINGS = fromString("strings");
        public static final BString INSERTIONS = fromString("insertions");
    }

    /**
     * Constants related to TypedValue fields.
     */
    public static final class TypedValueFields {
        private TypedValueFields() {}
        public static final BString VALUE = fromString("value");
    }

    /**
     * Constants related to SQL Types supported.
     */
    public static final class SqlTypes {
        private SqlTypes() {}
        public static final String VARCHAR = "VarcharValue";
        public static final String VARCHAR_ARRAY = "VarcharArrayValue";
        public static final String CHAR = "CharValue";
        public static final String CHAR_ARRAY = "CharArrayValue";
        public static final String TEXT = "TextValue";
        public static final String CLOB = "ClobValue";
        public static final String NCHAR = "NCharValue";
        public static final String NVARCHAR = "NVarcharValue";
        public static final String NVARCHAR_ARRAY = "NVarcharArrayValue";
        public static final String NCLOB = "NClobValue";
        public static final String SMALLINT = "SmallIntValue";
        public static final String SMALLINT_ARRAY = "SmallIntArrayValue";
        public static final String INTEGER = "IntegerValue";
        public static final String INTEGER_ARRAY = "IntegerArrayValue";
        public static final String BIGINT = "BigIntValue";
        public static final String BIGINT_ARRAY = "BigIntArrayValue";
        public static final String NUMERIC = "NumericValue";
        public static final String NUMERIC_ARRAY = "NumericArrayValue";
        public static final String DECIMAL = "DecimalValue";
        public static final String DECIMAL_ARRAY = "DecimalArrayValue";
        public static final String REAL = "RealValue";
        public static final String REAL_ARRAY = "RealArrayValue";
        public static final String FLOAT = "FloatValue";
        public static final String FLOAT_ARRAY = "FloatArrayValue";
        public static final String DOUBLE = "DoubleValue";
        public static final String DOUBLE_ARRAY = "DoubleArrayValue";
        public static final String BIT = "BitValue";
        public static final String BIT_ARRAY = "BitArrayValue";
        public static final String BOOLEAN = "BooleanValue";
        public static final String BOOLEAN_ARRAY = "BooleanArrayValue";
        public static final String BINARY = "BinaryValue";
        public static final String BINARY_ARRAY = "BinaryArrayValue";
        public static final String VARBINARY = "VarBinaryValue";
        public static final String VARBINARY_ARRAY = "VarBinaryArrayValue";
        public static final String BLOB = "BlobValue";
        public static final String DATE = "DateValue";
        public static final String DATE_ARRAY = "DateArrayValue";
        public static final String TIME = "TimeValue";
        public static final String TIME_ARRAY = "TimeArrayValue";
        public static final String DATETIME = "DateTimeValue";
        public static final String DATETIME_ARRAY = "DateTimeArrayValue";
        public static final String TIMESTAMP = "TimestampValue";
        public static final String TIMESTAMP_ARRAY = "TimestampArrayValue";
        public static final String ARRAY = "ArrayValue";
        public static final String REF = "RefValue";
        public static final String ROW = "RowValue";
        public static final String STRUCT = "StructValue";
        public static final String OPTIONAL_STRING = "string?";
        public static final String STRING = "string";
        public static final String OPTIONAL_BOOLEAN = "boolean?";
        public static final String BOOLEAN_TYPE = "boolean";
        public static final String OPTIONAL_FLOAT = "float?";
        public static final String FLOAT_TYPE = "float";
        public static final String OPTIONAL_INT = "int?";
        public static final String INT = "int";
        public static final String OPTIONAL_DECIMAL = "decimal?";
        public static final String DECIMAL_TYPE = "decimal";
        public static final String OPTIONAL_BYTE = "byte[]?";
        public static final String BYTE_ARRAY_TYPE = "byte[]";
        public static final String CIVIL_ARRAY_TYPE = "time:Civil";
        public static final String TIME_OF_DAY_ARRAY_TYPE = "time:TimeOfDay";
    }

    /**
     * Constants related to SQL Arrays supported.
     */
    public static final class SqlArrays {
        private SqlArrays() {}
        public static final String VARCHAR = "VARCHAR";
        public static final String CHAR = "CHAR";
        public static final String NVARCHAR = "NVARCHAR";
        public static final String SMALLINT = "SMALLINT";
        public static final String INTEGER = "INTEGER";
        public static final String BIGINT = "BIGINT";
        public static final String NUMERIC = "NUMERIC";
        public static final String DECIMAL = "DECIMAL";
        public static final String REAL = "REAL";
        public static final String FLOAT = "FLOAT";
        public static final String DOUBLE = "DOUBLE";
        public static final String BIT = "BIT";
        public static final String BOOLEAN = "BOOLEAN";
        public static final String BINARY = "BINARY";
        public static final String VARBINARY = "VARBINARY";
        public static final String DATE = "DATE";
        public static final String TIME = "TIME";
        public static final String DATETIME = "DATETIME";
        public static final String TIMESTAMP = "TIMESTAMP";
        public static final String TIME_WITH_TIMEZONE = "TIME_WITH_TIMEZONE";
        public static final String TIMESTAMP_WITH_TIMEZONE = "TIMESTAMP_WITH_TIMEZONE";
    }

    /**
     * Constants related to OutParameter supported.
     */
    public static final class OutParameterTypes {
        private OutParameterTypes() {}
        public static final String CHAR = "CharOutParameter";
        public static final String CHAR_ARRAY = "CharArrayOutParameter";
        public static final String VARCHAR = "VarcharOutParameter";
        public static final String VARCHAR_ARRAY = "VarcharArrayOutParameter";
        public static final String NCHAR = "NCharOutParameter";
        public static final String NVARCHAR = "NVarcharOutParameter";
        public static final String NVARCHAR_ARRAY = "NVarcharArrayOutParameter";
        public static final String BINARY = "BinaryOutParameter";
        public static final String BINARY_ARRAY = "BinaryArrayOutParameter";
        public static final String ARRAY = "ArrayOutParameter";
        public static final String VARBINARY = "VarBinaryOutParameter";
        public static final String VARBINARY_ARRAY = "VarBinaryArrayOutParameter";
        public static final String TEXT = "TextOutParameter";
        public static final String BLOB = "BlobOutParameter";
        public static final String CLOB = "ClobOutParameter";
        public static final String NCLOB = "NClobOutParameter";
        public static final String DATE = "DateOutParameter";
        public static final String DATE_ARRAY = "DateArrayOutParameter";
        public static final String TIME = "TimeOutParameter";
        public static final String TIME_ARRAY = "TimeArrayOutParameter";
        public static final String TIME_WITH_TIMEZONE = "TimeWithTimezoneOutParameter";
        public static final String TIME_WITH_TIMEZONE_ARRAY = "TimeWithTimezoneArrayOutParameter";
        public static final String DATE_TIME = "DateTimeOutParameter";
        public static final String DATE_TIME_ARRAY = "DateTimeArrayOutParameter";
        public static final String TIMESTAMP = "TimestampOutParameter";
        public static final String TIMESTAMP_ARRAY = "TimestampArrayOutParameter";
        public static final String TIMESTAMP_WITH_TIMEZONE = "TimestampWithTimezoneOutParameter";
        public static final String TIMESTAMP_WITH_TIMEZONE_ARRAY = "TimestampWithTimezoneArrayOutParameter";
        public static final String ROW = "RowOutParameter";
        public static final String SMALLINT = "SmallIntOutParameter";
        public static final String SMALL_INT_ARRAY = "SmallIntArrayOutParameter";
        public static final String INTEGER = "IntegerOutParameter";
        public static final String INTEGER_ARRAY = "IntegerArrayOutParameter";
        public static final String BIGINT = "BigIntOutParameter";
        public static final String BIGINT_ARRAY = "BigIntArrayOutParameter";
        public static final String REAL = "RealOutParameter";
        public static final String REAL_ARRAY = "RealArrayOutParameter";
        public static final String FLOAT = "FloatOutParameter";
        public static final String FLOAT_ARRAY = "FloatArrayOutParameter";
        public static final String DOUBLE = "DoubleOutParameter";
        public static final String DOUBLE_ARRAY = "DoubleArrayOutParameter";
        public static final String NUMERIC = "NumericOutParameter";
        public static final String NUMERIC_ARRAY = "NumericArrayOutParameter";
        public static final String DECIMAL = "DecimalOutParameter";
        public static final String DECIMAL_ARRAY = "DecimalArrayOutParameter";
        public static final String BIT = "BitOutParameter";
        public static final String BIT_ARRAY = "BitArrayOutParameter";
        public static final String BOOLEAN = "BooleanOutParameter";
        public static final String BOOLEAN_ARRAY = "BooleanArrayOutParameter";
        public static final String REF = "RefOutParameter";
        public static final String STRUCT = "StructOutParameter";
        public static final String XML = "XMLOutParameter";
    }

    /**
     * Constants for SQL Params.
     */
    public static final class SQLParamsFields {
        private SQLParamsFields() {}
        public static final BString URL = fromString("url");
        public static final BString USER = fromString("user");
        public static final BString PASSWORD = fromString("password");
        public static final BString DATASOURCE_NAME = fromString("datasourceName");
        public static final BString OPTIONS = fromString("options");
        public static final BString CONNECTION_POOL = fromString("connectionPool");
        public static final BString CONNECTION_POOL_OPTIONS = fromString("connectionPoolOptions");
    }

    /**
     * Constants for Procedure call parameter objects.
     */
    public static final class ParameterObject {
        private ParameterObject() {}
        public static final String INOUT_PARAMETER = "InOutParameter";
        public static final String OUT_PARAMETER = "OutParameter";
        public static final String SQL_TYPE_NATIVE_DATA = "sqlType";
        public static final String VALUE_NATIVE_DATA = "value";

        public static final BString IN_VALUE_FIELD = fromString("in");
    }

    /**
     * Constants for array types.
     */
    public static final class ArrayTypes {
        private ArrayTypes() {}
        public static final String STRING = "string[]";
        public static final String INTEGER = "int[]";
        public static final String FLOAT = "float[]";
        public static final String BOOLEAN = "boolean[]";
        public static final String DATE = "time:Date[]";
        public static final String CIVIL = "time:Civil[]";
        public static final String TIME_OF_DAY = "time:TimeOfDay[]";
        public static final String DECIMAL = "decimal[]";
        public static final String UTC = "([int,decimal] & readonly)[]";
        public static final String BYTE = "byte[][]";
    }

    /**
     * Constants for classes.
     */
    public static final class Classes {
        private Classes() {}
        public static final String STRING = "java.lang.String";
        public static final String BOOLEAN = "java.lang.Boolean";
        public static final String SHORT = "java.lang.Short";
        public static final String INTEGER = "java.lang.Integer";
        public static final String LONG = "java.lang.Long";
        public static final String FLOAT = "java.lang.Float";
        public static final String DOUBLE = "java.lang.Double";
        public static final String BIG_DECIMAL = "java.math.BigDecimal";
        public static final String BYTE = "byte[]";
        public static final String DATE = "java.sql.Date";
        public static final String TIMESTAMP = "java.sql.Timestamp";
        public static final String TIME = "java.sql.Time";
        public static final String OFFSET_TIME = "java.time.OffsetTime";
        public static final String OFFSET_DATE_TIME = "java.time.OffsetDateTime";
    }
}
