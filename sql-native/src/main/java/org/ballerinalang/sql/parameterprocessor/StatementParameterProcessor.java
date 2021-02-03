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

package org.ballerinalang.sql.parameterprocessor;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BXml;
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.stdlib.io.channels.base.Channel;
import org.ballerinalang.stdlib.io.channels.base.CharacterChannel;
import org.ballerinalang.stdlib.io.readers.CharacterChannelReader;
import org.ballerinalang.stdlib.io.utils.IOConstants;
import org.ballerinalang.stdlib.time.util.TimeUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.ballerinalang.sql.utils.Utils.throwInvalidParameterError;

/**
 * This class implements methods required convert ballerina types into SQL types and
 * other methods that process the parameters of the statement.
 */
public class StatementParameterProcessor extends AbstractStatementParameterProcessor {

    private static final Object lock = new Object();
    private static volatile StatementParameterProcessor instance;

    public static StatementParameterProcessor getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new StatementParameterProcessor();
                }
            }
        }
        return instance;
    }

    private int getSQLType(BObject typedValue) throws ApplicationError {
        String sqlType = typedValue.getType().getName();
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

    private void setSqlTypedParam(Connection connection, PreparedStatement preparedStatement, int index,
                                    BObject typedValue)
            throws SQLException, ApplicationError, IOException {
        String sqlType = typedValue.getType().getName();
        Object value = typedValue.get(Constants.TypedValueFields.VALUE);
        switch (sqlType) {
            case Constants.SqlTypes.VARCHAR:
                setVarchar(index, value.toString(), preparedStatement);
                break;
            case Constants.SqlTypes.CHAR:
                setChar(index, value, preparedStatement);
                break;
            case Constants.SqlTypes.TEXT:
                setText(index, value, preparedStatement);
                break;
            case Constants.SqlTypes.NCHAR:
                setNChar(index, value, preparedStatement);
                break;
            case Constants.SqlTypes.NVARCHAR:
                setNVarchar(index, value, preparedStatement);
                break;
            case Constants.SqlTypes.BIT:
                setBit(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.BOOLEAN:
                setBoolean(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.INTEGER:
                setInteger(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.BIGINT:
                setBigInt(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.SMALLINT:
                setSmallInt(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.FLOAT:
                setFloat(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.REAL:
                setReal(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.DOUBLE:
                setDouble(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.NUMERIC:
                setNumeric(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.DECIMAL:
                setDecimal(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.BINARY:
                setBinary(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.VARBINARY:
                setVarBinary(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.BLOB:
                setBlob(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.CLOB:
                setClob(index, value, preparedStatement, sqlType, connection);
                break;
            case Constants.SqlTypes.NCLOB:
                setNClob(index, value, preparedStatement, sqlType, connection);
                break;
            case Constants.SqlTypes.DATE:
                setDate(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.TIME:
                setTime(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.TIMESTAMP:
                setTimestamp(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.DATETIME:
                setDateTime(index, value, preparedStatement, sqlType);
                break;
            case Constants.SqlTypes.ARRAY:
                setArray(index, value, preparedStatement, connection);
                break;
            case Constants.SqlTypes.REF:
                setRef(index, value, preparedStatement, connection);
                break;
            case Constants.SqlTypes.STRUCT:
                setStruct(index, value, preparedStatement, connection);
                break;
            case Constants.SqlTypes.ROW:
                setRow(index, value, preparedStatement, sqlType);
                break;
            default:
                setCustomSqlTypedParam(connection, preparedStatement, index, typedValue);
        }
    }

    private Object[] getArrayData(Object value) throws ApplicationError {
        Type type = TypeUtils.getType(value);
        if (value == null || type.getTag() != TypeTags.ARRAY_TAG) {
            return new Object[]{null, null};
        }
        Type elementType = ((ArrayType) type).getElementType();
        int typeTag = elementType.getTag();
        Object[] arrayData;
        int arrayLength;
        switch (typeTag) {
            case TypeTags.INT_TAG:
                return getIntArrayData(value);
            case TypeTags.FLOAT_TAG:
                return getFloatArrayData(value);
            case TypeTags.DECIMAL_TAG:
                return getDecimalArrayData(value);
            case TypeTags.STRING_TAG:
                return getStringArrayData(value);
            case TypeTags.BOOLEAN_TAG:
                return getBooleanArrayData(value);
            case TypeTags.ARRAY_TAG:
                return getNestedArrayData(value);
            default:
                return getCustomArrayData(value);
        }
    }

    private Object[] getStructData(Object value, Connection conn) throws SQLException, ApplicationError {
        Type type = TypeUtils.getType(value);
        if (value == null || (type.getTag() != TypeTags.OBJECT_TYPE_TAG
                && type.getTag() != TypeTags.RECORD_TYPE_TAG)) {
            return new Object[]{null, null};
        }
        String structuredSQLType = type.getName().toUpperCase(Locale.getDefault());
        Map<String, Field> structFields = ((StructureType) type)
                .getFields();
        int fieldCount = structFields.size();
        Object[] structData = new Object[fieldCount];
        Iterator<Field> fieldIterator = structFields.values().iterator();
        for (int i = 0; i < fieldCount; ++i) {
            Field field = fieldIterator.next();
            Object bValue = ((BMap) value).get(fromString(field.getFieldName()));
            int typeTag = field.getFieldType().getTag();
            switch (typeTag) {
                case TypeTags.INT_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG:
                case TypeTags.DECIMAL_TAG:
                    structData[i] = bValue;
                    break;
                case TypeTags.ARRAY_TAG:
                    getArrayStructData(field, bValue, structData, i, structuredSQLType);
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    getRecordStructData(bValue, structData, conn, i);
                    break;
                default:
                    getCustomStructData(value, conn);
            }
        }
        return new Object[]{structData, structuredSQLType};
    }

    public void setParams(Connection connection, PreparedStatement preparedStatement, BObject paramString)
            throws SQLException, ApplicationError, IOException {
        BArray arrayValue = paramString.getArrayValue(Constants.ParameterizedQueryFields.INSERTIONS);
        for (int i = 0; i < arrayValue.size(); i++) {
            Object object = arrayValue.get(i);
            int index = i + 1;
            setSQLValueParam(connection, preparedStatement, object, index, false);
        }
    }

    public int setSQLValueParam(Connection connection, PreparedStatement preparedStatement,
                                   Object object, int index, boolean returnType)
            throws SQLException, ApplicationError, IOException {
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
            if (objectArray.getElementType().getTag() == org.wso2.ballerinalang.compiler.util.TypeTags.BYTE) {
                preparedStatement.setBytes(index, objectArray.getBytes());
            } else {
                throw new ApplicationError("Only byte[] is supported can be set directly into " +
                        "ParameterizedQuery, any other array types should be wrapped as sql:Value");
            }
            return Types.VARBINARY;
        } else if (object instanceof BObject) {
            BObject objectValue = (BObject) object;
            if ((objectValue.getType().getTag() == TypeTags.OBJECT_TYPE_TAG)) {
                setSqlTypedParam(connection, preparedStatement, index, objectValue);
                if (returnType) {
                    return getSQLType(objectValue);
                }
                return 0;
            } else {
                throw new ApplicationError("Unsupported type:" +
                        objectValue.getType().getQualifiedName() + " in column index: " + index);
            }
        } else if (object instanceof BXml) {
            preparedStatement.setObject(index, ((BXml) object).getTextValue(), Types.SQLXML);
            return Types.SQLXML;
        } else {
            throw new ApplicationError("Unsupported type passed in column index: " + index);
        }
    }

    private void setString(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException {
        if (value == null) {
            preparedStatement.setString(index, null);
        } else {
            preparedStatement.setString(index, value.toString());
        }
    }

    private void setNstring(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException {
        if (value == null) {
            preparedStatement.setNString(index, null);
        } else {
            preparedStatement.setNString(index, value.toString());
        }
    }

    private void setBooleanValue(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.BOOLEAN);
        } else if (value instanceof BString) {
            preparedStatement.setBoolean(index, Boolean.parseBoolean(value.toString()));
        } else if (value instanceof Integer || value instanceof Long) {
            long lVal = ((Number) value).longValue();
            if (lVal == 1 || lVal == 0) {
                preparedStatement.setBoolean(index, lVal == 1);
            } else {
                throw new ApplicationError("Only 1 or 0 can be passed for " + sqlType
                        + " SQL Type, but found :" + lVal);
            }
        } else if (value instanceof Boolean) {
            preparedStatement.setBoolean(index, (Boolean) value);
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    private void setFloatAndReal(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.FLOAT);
        } else if (value instanceof Double || value instanceof Long ||
                value instanceof Float || value instanceof Integer) {
            preparedStatement.setFloat(index, ((Number) value).floatValue());
        } else if (value instanceof BDecimal) {
            preparedStatement.setFloat(index, ((BDecimal) value).decimalValue().floatValue());
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    private void setNumericAndDecimal(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.DECIMAL);
        } else if (value instanceof Double || value instanceof Long) {
            preparedStatement.setBigDecimal(index, new BigDecimal(((Number) value).doubleValue(),
                    MathContext.DECIMAL64));
        } else if (value instanceof Integer || value instanceof Float) {
            preparedStatement.setBigDecimal(index, new BigDecimal(((Number) value).doubleValue(),
                    MathContext.DECIMAL32));
        } else if (value instanceof BDecimal) {
            preparedStatement.setBigDecimal(index, ((BDecimal) value).decimalValue());
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    private void setBinaryAndBlob(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError, IOException {
        if (value == null) {
            preparedStatement.setBytes(index, null);
        } else if (value instanceof BArray) {
            BArray arrayValue = (BArray) value;
            if (arrayValue.getElementType().getTag() == org.wso2.ballerinalang.compiler.util.TypeTags.BYTE) {
                preparedStatement.setBytes(index, arrayValue.getBytes());
            } else {
                throw throwInvalidParameterError(value, sqlType);
            }
        } else if (value instanceof BObject) {
            BObject objectValue = (BObject) value;
            if (objectValue.getType().getName().equalsIgnoreCase(Constants.READ_BYTE_CHANNEL_STRUCT) &&
                    objectValue.getType().getPackage().toString()
                            .equalsIgnoreCase(IOConstants.IO_PACKAGE_ID.toString())) {
                Channel byteChannel = (Channel) objectValue.getNativeData(IOConstants.BYTE_CHANNEL_NAME);
                preparedStatement.setBinaryStream(index, byteChannel.getInputStream());
            } else {
                throw throwInvalidParameterError(value, sqlType);
            }
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    private void setClobAndNclob(int index, Object value, PreparedStatement preparedStatement, String sqlType,
                                 Connection connection)
            throws SQLException, ApplicationError {
        Clob clob;
        if (value == null) {
            preparedStatement.setNull(index, Types.CLOB);
        } else {
            if (sqlType.equalsIgnoreCase(Constants.SqlTypes.NCLOB)) {
                clob = connection.createNClob();
            } else {
                clob = connection.createClob();
            }
            if (value instanceof BString) {
                clob.setString(1, value.toString());
                preparedStatement.setClob(index, clob);
            } else if (value instanceof BObject) {
                BObject objectValue = (BObject) value;
                if (objectValue.getType().getName().equalsIgnoreCase(Constants.READ_CHAR_CHANNEL_STRUCT) &&
                        objectValue.getType().getPackage().toString()
                                .equalsIgnoreCase(IOConstants.IO_PACKAGE_ID.toString())) {
                    CharacterChannel charChannel = (CharacterChannel) objectValue.getNativeData(
                            IOConstants.CHARACTER_CHANNEL_NAME);
                    preparedStatement.setCharacterStream(index, new CharacterChannelReader(charChannel));
                } else {
                    throw throwInvalidParameterError(value, sqlType);
                }
            }
        }
    }

    private void setRefAndStruct(int index, Object value, PreparedStatement preparedStatement, Connection connection)
            throws SQLException, ApplicationError {
        Object[] structData = getStructData(value, connection);
        Object[] dataArray = (Object[]) structData[0];
        String structuredSQLType = (String) structData[1];
        if (dataArray == null) {
            preparedStatement.setNull(index, Types.STRUCT);
        } else {
            Struct struct = connection.createStruct(structuredSQLType, dataArray);
            preparedStatement.setObject(index, struct);
        }
    }

    private void setDateTimeAndTimestamp(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setTimestamp(index, null);
        } else {
            Timestamp timestamp;
            if (value instanceof BString) {
                timestamp = Timestamp.valueOf(value.toString());
            } else if (value instanceof Long) {
                timestamp = new Timestamp((Long) value);
            } else if (value instanceof BMap) {
                BMap<BString, Object> dateTimeStruct = (BMap<BString, Object>) value;
                if (dateTimeStruct.getType().getName()
                        .equalsIgnoreCase(org.ballerinalang.stdlib.time.util.Constants.STRUCT_TYPE_TIME)) {
                    ZonedDateTime zonedDateTime = TimeUtils.getZonedDateTime(dateTimeStruct);
                    timestamp = new Timestamp(zonedDateTime.toInstant().toEpochMilli());
                } else {
                    throw throwInvalidParameterError(value, sqlType);
                }
            } else {
                throw throwInvalidParameterError(value, sqlType);
            }
            preparedStatement.setTimestamp(index, timestamp);
        }
    }

    @Override
    public int getCustomOutParameterType(BObject typedValue) throws ApplicationError {
        String sqlType = typedValue.getType().getName();
        throw new ApplicationError("Unsupported OutParameter type: " + sqlType);
    }

    @Override
    protected int getCustomSQLType(BObject typedValue) throws ApplicationError {
        String sqlType = typedValue.getType().getName();
        throw new ApplicationError("Unsupported SQL type: " + sqlType);
    }

    @Override
    protected void setCustomSqlTypedParam(Connection connection, PreparedStatement preparedStatement,
                                          int index, BObject typedValue)
            throws SQLException, ApplicationError, IOException {
        String sqlType = typedValue.getType().getName();
        throw new ApplicationError("Unsupported SQL type: " + sqlType);
    }

    @Override
    protected Object[] getCustomArrayData(Object value) throws ApplicationError {
        throw throwInvalidParameterError(value, Constants.SqlTypes.ARRAY);
    }

    @Override
    protected Object[] getCustomStructData(Object value, Connection conn)
            throws SQLException, ApplicationError {
        Type type = TypeUtils.getType(value);
        String structuredSQLType = type.getName().toUpperCase(Locale.getDefault());
        throw new ApplicationError("unsupported data type of " + structuredSQLType
                + " specified for struct parameter");
    }

    @Override
    protected void setVarchar(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException {
        setString(index, value, preparedStatement);
    }

    @Override
    protected void setText(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException {
        setString(index, value, preparedStatement);
    }

    @Override
    protected void setChar(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException {
        setString(index, value, preparedStatement);
    }

    @Override
    protected void setNChar(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException {
        setNstring(index, value, preparedStatement);
    }

    @Override
    protected void setNVarchar(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException {
        setNstring(index, value, preparedStatement);
    }

    @Override
    protected void setBit(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        setBooleanValue(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setBoolean(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        setBooleanValue(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setInteger(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.INTEGER);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setInt(index, ((Number) value).intValue());
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setBigInt(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.BIGINT);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setLong(index, ((Number) value).longValue());
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setSmallInt(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.SMALLINT);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setShort(index, ((Number) value).shortValue());
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setFloat(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        setFloatAndReal(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setReal(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        setFloatAndReal(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setDouble(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.DOUBLE);
        } else if (value instanceof Double || value instanceof Long ||
                value instanceof Float || value instanceof Integer) {
            preparedStatement.setDouble(index, ((Number) value).doubleValue());
        } else if (value instanceof BDecimal) {
            preparedStatement.setDouble(index, ((BDecimal) value).decimalValue().doubleValue());
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setNumeric(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        setNumericAndDecimal(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setDecimal(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        setNumericAndDecimal(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setBinary(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError, IOException {
        setBinaryAndBlob(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setVarBinary(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError, IOException {
        setBinaryAndBlob(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setBlob(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError, IOException {
        setBinaryAndBlob(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setClob(int index, Object value, PreparedStatement preparedStatement, String sqlType,
            Connection connection)
            throws SQLException, ApplicationError {
        setClobAndNclob(index, value, preparedStatement, sqlType, connection);
    }

    @Override
    protected void setNClob(int index, Object value, PreparedStatement preparedStatement, String sqlType,
            Connection connection)
            throws SQLException, ApplicationError {
        setClobAndNclob(index, value, preparedStatement, sqlType, connection);
    }

    @Override
    protected void setRow(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setRowId(index, null);
        } else if (value instanceof BArray) {
            BArray arrayValue = (BArray) value;
            if (arrayValue.getElementType().getTag() == org.wso2.ballerinalang.compiler.util.TypeTags.BYTE) {
                RowId rowId = arrayValue::getBytes;
                preparedStatement.setRowId(index, rowId);
            } else {
                throw throwInvalidParameterError(value, sqlType);
            }
        } else {
            throw throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setStruct(int index, Object value, PreparedStatement preparedStatement, Connection connection)
            throws SQLException, ApplicationError {
        setRefAndStruct(index, value, preparedStatement, connection);
    }

    @Override
    protected void setRef(int index, Object value, PreparedStatement preparedStatement, Connection connection)
            throws SQLException, ApplicationError {
        setRefAndStruct(index, value, preparedStatement, connection);
    }

    @Override
    protected void setArray(int index, Object value, PreparedStatement preparedStatement, Connection connection)
            throws SQLException, ApplicationError {
        Object[] arrayData = getArrayData(value);
        if (arrayData[0] != null) {
            Array array = connection.createArrayOf((String) arrayData[1], (Object[]) arrayData[0]);
            preparedStatement.setArray(index, array);
        } else {
            preparedStatement.setArray(index, null);
        }
    }

    @Override
    protected void setDateTime(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        setDateTimeAndTimestamp(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setTimestamp(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        setDateTimeAndTimestamp(index, value, preparedStatement, sqlType);
    }

    @Override
    protected void setDate(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        Date date;
        if (value == null) {
            preparedStatement.setDate(index, null);
        } else {
            if (value instanceof BString) {
                date = Date.valueOf(value.toString());
            } else if (value instanceof Long) {
                date = new Date((Long) value);
            } else if (value instanceof BMap) {
                BMap<BString, Object> dateTimeStruct = (BMap<BString, Object>) value;
                if (dateTimeStruct.getType().getName()
                        .equalsIgnoreCase(org.ballerinalang.stdlib.time.util.Constants.STRUCT_TYPE_TIME)) {
                    ZonedDateTime zonedDateTime = TimeUtils.getZonedDateTime(dateTimeStruct);
                    date = new Date(zonedDateTime.toInstant().toEpochMilli());
                } else {
                    throw throwInvalidParameterError(value, sqlType);
                }
            } else {
                throw throwInvalidParameterError(value, sqlType);
            }
            preparedStatement.setDate(index, date);
        }
    }

    @Override
    protected void setTime(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setTime(index, null);
        } else {
            Time time;
            if (value instanceof BString) {
                time = Time.valueOf(value.toString());
            } else if (value instanceof Long) {
                time = new Time((Long) value);
            } else if (value instanceof BMap) {
                BMap<BString, Object> dateTimeStruct = (BMap<BString, Object>) value;
                if (dateTimeStruct.getType().getName()
                        .equalsIgnoreCase(org.ballerinalang.stdlib.time.util.Constants.STRUCT_TYPE_TIME)) {
                    ZonedDateTime zonedDateTime = TimeUtils.getZonedDateTime(dateTimeStruct);
                    time = new Time(zonedDateTime.toInstant().toEpochMilli());
                } else {
                    throw throwInvalidParameterError(value, sqlType);
                }
            } else {
                throw throwInvalidParameterError(value, sqlType);
            }
            preparedStatement.setTime(index, time);
        }
    }

    @Override
    protected Object[] getIntArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new Long[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BArray) value).getInt(i);
        }
        return new Object[]{arrayData, "BIGINT"};
    }

    @Override
    protected Object[] getFloatArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new Double[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BArray) value).getFloat(i);
        }
        return new Object[]{arrayData, "DOUBLE"};
    }

    @Override
    protected Object[] getDecimalArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new BigDecimal[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BDecimal) ((BArray) value).getRefValue(i)).value();
        }
        return new Object[]{arrayData, "DECIMAL"};
    }

    @Override
    protected Object[] getStringArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new String[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BArray) value).getBString(i).getValue();
        }
        return new Object[]{arrayData, "VARCHAR"};
    }

    @Override
    protected Object[] getBooleanArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new Boolean[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BArray) value).getBoolean(i);
        }
        return new Object[]{arrayData, "BOOLEAN"};
    }

    @Override
    protected Object[] getNestedArrayData(Object value) throws ApplicationError {
        Type type = TypeUtils.getType(value);
        Type elementType = ((ArrayType) type).getElementType();
        Type elementTypeOfArrayElement = ((ArrayType) elementType)
                .getElementType();
        if (elementTypeOfArrayElement.getTag() == TypeTags.BYTE_TAG) {
            BArray arrayValue = (BArray) value;
            Object[] arrayData = new byte[arrayValue.size()][];
            for (int i = 0; i < arrayData.length; i++) {
                arrayData[i] = ((BArray) arrayValue.get(i)).getBytes();
            }
            return new Object[]{arrayData, "BINARY"};
        } else {
            throw throwInvalidParameterError(value, Constants.SqlTypes.ARRAY);
        }
    }

    @Override
    protected void getRecordStructData(Object bValue, Object[] structData, Connection conn, int i)
            throws SQLException, ApplicationError {
        Object structValue = bValue;
        Object[] internalStructData = getStructData(structValue, conn);
        Object[] dataArray = (Object[]) internalStructData[0];
        String internalStructType = (String) internalStructData[1];
        structValue = conn.createStruct(internalStructType, dataArray);
        structData[i] = structValue;
    }

    @Override
    protected void getArrayStructData(Field field, Object bValue, Object[] structData, int i, String structuredSQLType)
            throws SQLException, ApplicationError {
        Type elementType = ((ArrayType) field
                .getFieldType()).getElementType();
        if (elementType.getTag() == TypeTags.BYTE_TAG) {
            structData[i] = ((BArray) bValue).getBytes();
        } else {
            throw new ApplicationError("unsupported data type of " + structuredSQLType
                    + " specified for struct parameter");
        }
    }

}
