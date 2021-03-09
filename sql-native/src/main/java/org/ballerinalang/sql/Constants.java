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

package org.ballerinalang.sql;

import io.ballerina.runtime.api.values.BString;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;

/**
 * Constants for SQL client.
 *
 * @since 1.2.0
 */
public final class Constants {

    public static final String CONNECTOR_NAME = "SQLClientConnector";
    public static final String DATABASE_CLIENT = "Client";
    public static final String SQL_CONNECTOR_TRANSACTION_ID = "sql-transaction-id";

    public static final String BATCH_EXECUTE_ERROR_DETAIL = "BatchExecuteErrorDetail";
    public static final String BATCH_EXECUTE_ERROR = "BatchExecuteError";
    public static final String BATCH_EXECUTE_ERROR_MESSAGE = "Error occurred when batch executing commands.";
    public static final String DATABASE_ERROR_DETAILS = "DatabaseErrorDetail";
    public static final String DATABASE_ERROR = "DatabaseError";
    public static final String APPLICATION_ERROR = "ApplicationError";
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
        public static final BString MAX_OPEN_CONNECTIONS = fromString("maxOpenConnections");
        public static final BString MAX_CONNECTION_LIFE_TIME = fromString(
                "maxConnectionLifeTime");
        public static final BString MIN_IDLE_CONNECTIONS = fromString("minIdleConnections");
    }

    /**
     * Constants related to database options.
     */
    public static final class Options {
        public static final BString URL = fromString("url");
    }

    /**
     * Constant related error fields.
     */
    public static final class ErrorRecordFields {
        public static final String ERROR_CODE = "errorCode";
        public static final String SQL_STATE = "sqlState";
        public static final String EXECUTION_RESULTS = "executionResults";

    }

    /**
     * Constants related to parameterized string fields.
     */
    public static final class ParameterizedQueryFields {
        public static final BString STRINGS = fromString("strings");
        public static final BString INSERTIONS = fromString("insertions");
    }

    /**
     * Constants related to TypedValue fields.
     */
    public static final class TypedValueFields {
        public static final BString VALUE = fromString("value");
    }

    /**
     * Constants related to SQL Types supported.
     */
    public static final class SqlTypes {
        public static final String VARCHAR = "VarcharValue";
        public static final String CHAR = "CharValue";
        public static final String TEXT = "TextValue";
        public static final String CLOB = "ClobValue";
        public static final String NCHAR = "NCharValue";
        public static final String NVARCHAR = "NVarcharValue";
        public static final String NCLOB = "NClobValue";
        public static final String SMALLINT = "SmallIntValue";
        public static final String INTEGER = "IntegerValue";
        public static final String BIGINT = "BigIntValue";
        public static final String NUMERIC = "NumericValue";
        public static final String DECIMAL = "DecimalValue";
        public static final String REAL = "RealValue";
        public static final String FLOAT = "FloatValue";
        public static final String DOUBLE = "DoubleValue";
        public static final String BIT = "BitValue";
        public static final String BOOLEAN = "BooleanValue";
        public static final String BINARY = "BinaryValue";
        public static final String VARBINARY = "VarBinaryValue";
        public static final String BLOB = "BlobValue";
        public static final String DATE = "DateValue";
        public static final String TIME = "TimeValue";
        public static final String DATETIME = "DateTimeValue";
        public static final String TIMESTAMP = "TimestampValue";
        public static final String ARRAY = "ArrayValue";
        public static final String REF = "RefValue";
        public static final String ROW = "RowValue";
        public static final String STRUCT = "StructValue";

    }

    /**
     * Constants related to OutParameter supported.
     */
    public static final class OutParameterTypes {
        public static final String CHAR = "CharOutParameter";
        public static final String VARCHAR = "VarcharOutParameter";
        public static final String NCHAR = "NCharOutParameter";
        public static final String NVARCHAR = "NVarcharOutParameter";
        public static final String BINARY = "BinaryOutParameter";
        public static final String VARBINARY = "VarBinaryOutParameter";
        public static final String TEXT = "TextOutParameter";
        public static final String BLOB = "BlobOutParameter";
        public static final String CLOB = "ClobOutParameter";
        public static final String NCLOB = "NClobOutParameter";
        public static final String DATE = "DateOutParameter";
        public static final String TIME = "TimeOutParameter";
        public static final String DATETIME = "DateTimeOutParameter";
        public static final String TIMESTAMP = "TimestampOutParameter";
        public static final String ARRAY = "ArrayOutParameter";
        public static final String ROW = "RowOutParameter";
        public static final String SMALLINT = "SmallIntOutParameter";
        public static final String INTEGER = "IntegerOutParameter";
        public static final String BIGINT = "BigIntOutParameter";
        public static final String REAL = "RealOutParameter";
        public static final String FLOAT = "FloatOutParameter";
        public static final String DOUBLE = "DoubleOutParameter";
        public static final String NUMERIC = "NumericOutParameter";
        public static final String DECIMAL = "DecimalOutParameter";
        public static final String BIT = "BitOutParameter";
        public static final String BOOLEAN = "BooleanOutParameter";
        public static final String REF = "RefOutParameter";
        public static final String STRUCT = "StructOutParameter";
        public static final String XML = "XMLOutParameter";
    }

    /**
     * Constants for SQL Params.
     */
    public static final class SQLParamsFields {
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
        public static final String INOUT_PARAMETER = "InOutParameter";
        public static final String OUT_PARAMETER = "OutParameter";
        public static final String SQL_TYPE_NATIVE_DATA = "sqlType";
        public static final String VALUE_NATIVE_DATA = "value";

        public static final BString IN_VALUE_FIELD = fromString("in");
    }

}
