/*
 *  Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.ballerina.stdlib.sql.parameterprocessor;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BXml;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.DataError;
import io.ballerina.stdlib.sql.exception.UnsupportedTypeError;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * This class has abstract implementation of methods required process the JDBC Prepared Statement.
 * Setters are used to set designated parameters to the PreparedStatement.
 *
 * @since 0.5.6
 */
public abstract class AbstractStatementParameterProcessor {

    public abstract int getCustomOutParameterType(BObject typedValue) throws DataError, SQLException;

    protected abstract int getCustomSQLType(BObject typedValue) throws DataError, SQLException;

    protected abstract void setCustomSqlTypedParam(Connection connection, PreparedStatement preparedStatement,
                                                   int index, BObject typedValue) throws DataError, SQLException;

    protected abstract void setVarchar(PreparedStatement preparedStatement, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setVarcharArray(Connection conn, PreparedStatement preparedStatement, int index,
                                            Object value) throws DataError, SQLException;

    protected abstract void setText(PreparedStatement preparedStatement, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setChar(PreparedStatement preparedStatement, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setCharArray(Connection conn, PreparedStatement preparedStatement, int index,
                                         Object value) throws DataError, SQLException;

    protected abstract void setNChar(PreparedStatement preparedStatement, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setNVarchar(PreparedStatement preparedStatement, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setNVarcharArray(Connection conn, PreparedStatement preparedStatement, int index,
                                             Object value) throws DataError, SQLException;

    protected abstract void setBit(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setBitArray(Connection conn, PreparedStatement preparedStatement, int index,
                                        Object value) throws DataError, SQLException;

    protected abstract void setBoolean(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setBooleanArray(Connection conn, PreparedStatement preparedStatement, int index,
                                            Object value) throws DataError, SQLException;

    protected abstract void setInteger(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setIntegerArray(Connection conn, PreparedStatement preparedStatement, int index,
                                            Object value) throws DataError, SQLException;

    protected abstract void setBigInt(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setBigIntArray(Connection conn, PreparedStatement preparedStatement, int index,
                                           Object value) throws DataError, SQLException;

    protected abstract void setSmallInt(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setSmallIntArray(Connection conn, PreparedStatement preparedStatement, int index,
                                             Object value) throws DataError, SQLException;

    protected abstract void setFloat(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setFloatArray(Connection conn, PreparedStatement preparedStatement, int index,
                                          Object value) throws DataError, SQLException;

    protected abstract void setReal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setRealArray(Connection conn, PreparedStatement preparedStatement, int index,
                                         Object value) throws DataError, SQLException;

    protected abstract void setDouble(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setDoubleArray(Connection conn, PreparedStatement preparedStatement, int index,
                                           Object value) throws DataError, SQLException;

    protected abstract void setNumeric(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setNumericArray(Connection conn, PreparedStatement preparedStatement, int index,
                                            Object value) throws DataError, SQLException;

    protected abstract void setDecimal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setDecimalArray(Connection conn, PreparedStatement preparedStatement, int index,
                                            Object value) throws DataError, SQLException;

    protected abstract void setBinary(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setBinaryArray(Connection conn, PreparedStatement preparedStatement, int index,
                                           Object value) throws DataError, SQLException;

    protected abstract void setVarBinary(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setVarBinaryArray(Connection conn, PreparedStatement preparedStatement, int index,
                                              Object value) throws DataError, SQLException;

    protected abstract void setBlob(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setClob(Connection connection, PreparedStatement preparedStatement, String sqlType,
                                    int index, Object value) throws DataError, SQLException;

    protected abstract void setNClob(Connection connection, PreparedStatement preparedStatement, String sqlType,
                                     int index, Object value) throws DataError, SQLException;

    protected abstract void setRow(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setStruct(Connection connection, PreparedStatement preparedStatement, int index,
                                      Object value) throws DataError, SQLException;

    protected abstract void setRef(Connection connection, PreparedStatement preparedStatement, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setArray(Connection connection, PreparedStatement preparedStatement, int index,
                                     Object value) throws DataError, SQLException;

    protected abstract void setDateTime(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setDateTimeArray(Connection conn, PreparedStatement preparedStatement, int index,
                                             Object value) throws DataError, SQLException;

    protected abstract void setTimestamp(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setTimestampArray(Connection conn, PreparedStatement preparedStatement, int index,
                                              Object value) throws DataError, SQLException;

    protected abstract void setDate(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setDateArray(Connection conn, PreparedStatement preparedStatement, int index,
                                         Object value) throws DataError, SQLException;

    protected abstract void setTime(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws DataError, SQLException;

    protected abstract void setTimeArray(Connection conn, PreparedStatement preparedStatement, int index,
                                         Object value) throws DataError, SQLException;

    protected abstract void setXml(Connection connection, PreparedStatement preparedStatement, int index, BXml value)
            throws DataError, SQLException;

    protected abstract int setCustomBOpenRecord(Connection connection, PreparedStatement preparedStatement, int index,
                                                Object value, boolean returnType) throws DataError, SQLException;

    public void setParams(Connection connection, PreparedStatement preparedStatement,
                          Object[] insertions) throws DataError, SQLException {
        for (int i = 0; i < insertions.length; i++) {
            Object object = insertions[i];
            int index = i + 1;
            setSQLValueParam(connection, preparedStatement, index, object, false);
        }
    }

    public int setSQLValueParam(Connection connection, PreparedStatement preparedStatement, int index, Object object,
                                boolean returnType) throws DataError, SQLException {
        try {
            if (object == null) {
                preparedStatement.setNull(index, Types.NULL);
                return Types.NULL;
            } else if (object instanceof BString) {
                preparedStatement.setString(index, object.toString());
                return Types.VARCHAR;
            } else if (object instanceof Long) {
                preparedStatement.setLong(index, (Long) object);
                return Types.BIGINT;
            } else if (object instanceof Double) {
                preparedStatement.setDouble(index, (Double) object);
                return Types.DOUBLE;
            } else if (object instanceof BDecimal) {
                preparedStatement.setBigDecimal(index, ((BDecimal) object).decimalValue());
                return Types.NUMERIC;
            } else if (object instanceof Boolean) {
                preparedStatement.setBoolean(index, (Boolean) object);
                return Types.BOOLEAN;
            } else if (object instanceof BArray) {
                BArray objectArray = (BArray) object;

                // If the type passed is time:Utc
                if (objectArray.getType().toString().equals(Constants.SqlTypes.UTC)) {
                    setTimestamp(preparedStatement, objectArray.getType().getName(), index, objectArray);
                    return Types.TIMESTAMP;
                }

                String type = objectArray.getElementType().toString();
                if (objectArray.getElementType().getTag() == TypeTags.BYTE_TAG) {
                    preparedStatement.setBytes(index, objectArray.getBytes());
                } else if (objectArray.getElementType().getTag() == TypeTags.ARRAY_TAG ||
                        type.equals(Constants.SqlTypes.OPTIONAL_BYTE) ||
                        type.equals(Constants.SqlTypes.BYTE_ARRAY_TYPE)) {
                    setBinaryArray(connection, preparedStatement, index, objectArray);
                } else if (type.equals(Constants.SqlTypes.STRING) || type.equals(Constants.SqlTypes.OPTIONAL_STRING)) {
                    setVarcharArray(connection, preparedStatement, index, objectArray);
                } else if (type.equals(Constants.SqlTypes.INT) || type.equals(Constants.SqlTypes.OPTIONAL_INT)) {
                    setIntegerArray(connection, preparedStatement, index, objectArray);
                } else if (type.equals(Constants.SqlTypes.BOOLEAN_TYPE) ||
                        type.equals(Constants.SqlTypes.OPTIONAL_BOOLEAN)) {
                    setBooleanArray(connection, preparedStatement, index, objectArray);
                } else if (type.equals(Constants.SqlTypes.FLOAT_TYPE) ||
                        type.equals(Constants.SqlTypes.OPTIONAL_FLOAT)) {
                    setFloatArray(connection, preparedStatement, index, objectArray);
                } else if (type.equals(Constants.SqlTypes.DECIMAL_TYPE) ||
                        type.equals(Constants.SqlTypes.OPTIONAL_DECIMAL)) {
                    setDecimalArray(connection, preparedStatement, index, objectArray);
                } else {
                    // Cannot be reached as this is validated with `ArrayValueType` in ballerina
                    throw new UnsupportedTypeError(type + "array type", index);
                }
                return Types.ARRAY;
            } else if (object instanceof BObject) {
                BObject objectValue = (BObject) object;
                setSqlTypedParam(connection, preparedStatement, index, objectValue);
                if (returnType) {
                    return getSQLType(objectValue);
                }
                return 0;
            } else if (object instanceof BXml) {
                setXml(connection, preparedStatement, index, (BXml) object);
                return Types.SQLXML;
            } else if (object instanceof BMap) {
                return setBMapParams(connection, preparedStatement, index, (BMap) object, returnType);
            } else {
                // Cannot be achieved since this is validated at compiler for `Value`
                throw new UnsupportedTypeError(object.getClass().getName(), index);
            }
        } catch (SQLException e) {
            String msg = e.getMessage();
            if (msg != null) {
                if (msg.contains("data exception") || msg.contains("incompatible data type")) {
                    throw new DataError(String.format("Error while constructing SQL query. %s: %s",
                            e.getMessage(), object));
                }
                throw e;
            }
            throw e;
        }
    }

    private void setSqlTypedParam(Connection connection, PreparedStatement preparedStatement, int index,
                                  BObject typedValue)
            throws DataError, SQLException {
        String sqlType = TypeUtils.getType(typedValue).getName();
        Object value = typedValue.get(Constants.TypedValueFields.VALUE);
        switch (sqlType) {
            case Constants.SqlTypes.VARCHAR:
                setVarchar(preparedStatement, index, value);
                break;
            case Constants.SqlTypes.VARCHAR_ARRAY:
                setVarcharArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.CHAR:
                setChar(preparedStatement, index, value);
                break;
            case Constants.SqlTypes.CHAR_ARRAY:
                setCharArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.TEXT:
                setText(preparedStatement, index, value);
                break;
            case Constants.SqlTypes.NCHAR:
                setNChar(preparedStatement, index, value);
                break;
            case Constants.SqlTypes.NVARCHAR_ARRAY:
                setNVarcharArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.NVARCHAR:
                setNVarchar(preparedStatement, index, value);
                break;
            case Constants.SqlTypes.BIT:
                setBit(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.BIT_ARRAY:
                setBitArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.BOOLEAN:
                setBoolean(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.BOOLEAN_ARRAY:
                setBooleanArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.INTEGER:
                setInteger(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.INTEGER_ARRAY:
                setIntegerArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.BIGINT:
                setBigInt(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.BIGINT_ARRAY:
                setBigIntArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.SMALLINT:
                setSmallInt(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.SMALLINT_ARRAY:
                setSmallIntArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.FLOAT:
                setFloat(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.FLOAT_ARRAY:
                setFloatArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.REAL:
                setReal(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.REAL_ARRAY:
                setRealArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.DOUBLE:
                setDouble(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.DOUBLE_ARRAY:
                setDoubleArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.NUMERIC:
                setNumeric(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.NUMERIC_ARRAY:
                setNumericArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.DECIMAL:
                setDecimal(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.DECIMAL_ARRAY:
                setDecimalArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.BINARY:
                setBinary(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.BINARY_ARRAY:
                setBinaryArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.VARBINARY:
                setVarBinary(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.VARBINARY_ARRAY:
                setVarBinaryArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.BLOB:
                setBlob(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.CLOB:
                setClob(connection, preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.NCLOB:
                setNClob(connection, preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.DATE:
                setDate(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.DATE_ARRAY:
                setDateArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.TIME:
                setTime(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.TIME_ARRAY:
                setTimeArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.TIMESTAMP:
                setTimestamp(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.TIMESTAMP_ARRAY:
                setTimestampArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.DATETIME:
                setDateTime(preparedStatement, sqlType, index, value);
                break;
            case Constants.SqlTypes.DATETIME_ARRAY:
                setDateTimeArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.ARRAY:
                setArray(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.REF:
                setRef(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.STRUCT:
                setStruct(connection, preparedStatement, index, value);
                break;
            case Constants.SqlTypes.ROW:
                setRow(preparedStatement, sqlType, index, value);
                break;
            default:
                setCustomSqlTypedParam(connection, preparedStatement, index, typedValue);
        }
    }

    private int getSQLType(BObject typedValue) throws DataError, SQLException {
        String sqlType = TypeUtils.getType(typedValue).getName();
        int sqlTypeValue;
        switch (sqlType) {
            case Constants.SqlTypes.VARCHAR:
            case Constants.SqlTypes.TEXT:
                sqlTypeValue = Types.VARCHAR;
                break;
            case Constants.SqlTypes.CHAR:
                sqlTypeValue = Types.CHAR;
                break;
            case Constants.SqlTypes.NCHAR:
                sqlTypeValue = Types.NCHAR;
                break;
            case Constants.SqlTypes.NVARCHAR:
                sqlTypeValue = Types.NVARCHAR;
                break;
            case Constants.SqlTypes.BIT:
                sqlTypeValue = Types.BIT;
                break;
            case Constants.SqlTypes.BOOLEAN:
                sqlTypeValue = Types.BOOLEAN;
                break;
            case Constants.SqlTypes.INTEGER:
                sqlTypeValue = Types.INTEGER;
                break;
            case Constants.SqlTypes.BIGINT:
                sqlTypeValue = Types.BIGINT;
                break;
            case Constants.SqlTypes.SMALLINT:
                sqlTypeValue = Types.SMALLINT;
                break;
            case Constants.SqlTypes.FLOAT:
                sqlTypeValue = Types.FLOAT;
                break;
            case Constants.SqlTypes.REAL:
                sqlTypeValue = Types.REAL;
                break;
            case Constants.SqlTypes.DOUBLE:
                sqlTypeValue = Types.DOUBLE;
                break;
            case Constants.SqlTypes.NUMERIC:
                sqlTypeValue = Types.NUMERIC;
                break;
            case Constants.SqlTypes.DECIMAL:
                sqlTypeValue = Types.DECIMAL;
                break;
            case Constants.SqlTypes.BINARY:
                sqlTypeValue = Types.BINARY;
                break;
            case Constants.SqlTypes.VARBINARY:
                sqlTypeValue = Types.VARBINARY;
                break;
            case Constants.SqlTypes.BLOB:
                if (typedValue instanceof BArray) {
                    sqlTypeValue = Types.VARBINARY;
                } else {
                    sqlTypeValue = Types.LONGVARBINARY;
                }
                break;
            case Constants.SqlTypes.CLOB:
            case Constants.SqlTypes.NCLOB:
                if (typedValue instanceof BString) {
                    sqlTypeValue = Types.CLOB;
                } else {
                    sqlTypeValue = Types.LONGVARCHAR;
                }
                break;
            case Constants.SqlTypes.DATE:
                sqlTypeValue = Types.DATE;
                break;
            case Constants.SqlTypes.TIME:
                sqlTypeValue = Types.TIME;
                break;
            case Constants.SqlTypes.TIMESTAMP:
            case Constants.SqlTypes.DATETIME:
                sqlTypeValue = Types.TIMESTAMP;
                break;
            case Constants.SqlTypes.ARRAY:
            case Constants.SqlTypes.SMALLINT_ARRAY:
            case Constants.SqlTypes.INTEGER_ARRAY:
            case Constants.SqlTypes.REAL_ARRAY:
            case Constants.SqlTypes.BIGINT_ARRAY:
            case Constants.SqlTypes.DOUBLE_ARRAY:
            case Constants.SqlTypes.FLOAT_ARRAY:
            case Constants.SqlTypes.BINARY_ARRAY:
            case Constants.SqlTypes.BOOLEAN_ARRAY:
            case Constants.SqlTypes.DECIMAL_ARRAY:
            case Constants.SqlTypes.NUMERIC_ARRAY:
            case Constants.SqlTypes.NVARCHAR_ARRAY:
            case Constants.SqlTypes.VARBINARY_ARRAY:
            case Constants.SqlTypes.VARCHAR_ARRAY:
            case Constants.SqlTypes.DATE_ARRAY:
            case Constants.SqlTypes.DATETIME_ARRAY:
            case Constants.SqlTypes.TIME_ARRAY:
            case Constants.SqlTypes.TIMESTAMP_ARRAY:
            case Constants.SqlTypes.BYTE_ARRAY_TYPE:
            case Constants.SqlTypes.CHAR_ARRAY:
            case Constants.SqlTypes.TIME_OF_DAY_ARRAY_TYPE:
            case Constants.SqlTypes.CIVIL_ARRAY_TYPE:
            case Constants.SqlTypes.BIT_ARRAY:
                sqlTypeValue = Types.ARRAY;
                break;
            case Constants.SqlTypes.REF:
                sqlTypeValue = Types.REF;
                break;
            case Constants.SqlTypes.STRUCT:
                sqlTypeValue = Types.STRUCT;
                break;
            case Constants.SqlTypes.ROW:
                sqlTypeValue = Types.ROWID;
                break;
            default:
                sqlTypeValue = getCustomSQLType(typedValue);
        }
        return sqlTypeValue;
    }

    private int setBMapParams(Connection connection, PreparedStatement preparedStatement, int index,
                              BMap value, boolean returnType) throws DataError, SQLException {
        String sqlType = value.getType().getName();
        int sqlTypeValue;
        switch (sqlType) {
            case Constants.SqlTypes.CIVIL:
                setTimestamp(preparedStatement, sqlType, index, value);
                sqlTypeValue = Types.TIMESTAMP;
                break;
            case Constants.SqlTypes.DATE_RECORD:
                setDate(preparedStatement, sqlType, index, value);
                sqlTypeValue = Types.DATE;
                break;
            case Constants.SqlTypes.TIME_RECORD:
                setTime(preparedStatement, sqlType, index, value);
                sqlTypeValue = Types.TIME;
                break;
            default:
                sqlTypeValue = setCustomBOpenRecord(connection, preparedStatement, index, value, returnType);
        }
        return sqlTypeValue;
    }
}
