/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.ballerina.stdlib.sql.compiler;

/**
 * Constants used in compiler plugin.
 */
public class Constants {
    public static final String BALLERINA = "ballerina";
    public static final String SQL = "sql";
    public static final String CONNECTION_POOL = "ConnectionPool";

    /**
     * Constants for fields in sql:ConnectionPool.
     */
    public static class ConnectionPool {
        public static final String MAX_OPEN_CONNECTIONS = "maxOpenConnections";
        public static final String MAX_CONNECTION_LIFE_TIME = "maxConnectionLifeTime";
        public static final String MIN_IDLE_CONNECTIONS = "minIdleConnections";
    }

    /**
     * Constants for fields in OutParameter objects.
     */
    public static class OutParameter {
        public static final String METHOD_NAME = "get";

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

    public static final String UNNECESSARY_CHARS_REGEX = "\"|\\n";

}
