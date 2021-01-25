/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.ballerinalang.sql.utils;

import io.ballerina.runtime.api.Module;
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.flags.SymbolFlags;
import io.ballerina.runtime.api.flags.TypeFlags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.utils.XmlUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BValue;
import io.ballerina.runtime.api.values.BXml;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.stdlib.io.channels.base.Channel;
import org.ballerinalang.stdlib.io.channels.base.CharacterChannel;
import org.ballerinalang.stdlib.io.readers.CharacterChannelReader;
import org.ballerinalang.stdlib.io.utils.IOConstants;
import org.ballerinalang.stdlib.time.util.TimeUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.ballerinalang.sql.Constants.AFFECTED_ROW_COUNT_FIELD;
import static org.ballerinalang.sql.Constants.EXECUTION_RESULT_FIELD;
import static org.ballerinalang.sql.Constants.EXECUTION_RESULT_RECORD;
import static org.ballerinalang.sql.Constants.LAST_INSERTED_ID_FIELD;

/**
 * This class has the utility methods to process and convert the SQL types into ballerina types,
 * and other shared utility methods.
 *
 * @since 1.2.0
 */
class Utils {

    private static final ArrayType stringArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_STRING);
    private static final ArrayType booleanArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_BOOLEAN);
    private static final ArrayType intArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_INT);
    private static final ArrayType floatArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_FLOAT);
    private static final ArrayType decimalArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_DECIMAL);

    static void closeResources(TransactionResourceManager trxResourceManager, ResultSet resultSet, Statement statement,
                               Connection connection) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException ignored) {
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException ignored) {
            }
        }
        if (trxResourceManager == null || !trxResourceManager.isInTransaction() ||
                !trxResourceManager.getCurrentTransactionContext().hasTransactionBlock()) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    static String getSqlQuery(BObject paramString) {
        BArray stringsArray = paramString.getArrayValue(Constants.ParameterizedQueryFields.STRINGS);
        StringBuilder sqlQuery = new StringBuilder();
        for (int i = 0; i < stringsArray.size(); i++) {
            if (i > 0) {
                sqlQuery.append(" ? ");
            }
            sqlQuery.append(stringsArray.get(i).toString());
        }
        return sqlQuery.toString();
    }

    static void setParams(Connection connection, PreparedStatement preparedStatement, BObject paramString)
            throws SQLException, ApplicationError, IOException {
        BArray arrayValue = paramString.getArrayValue(Constants.ParameterizedQueryFields.INSERTIONS);
        for (int i = 0; i < arrayValue.size(); i++) {
            Object object = arrayValue.get(i);
            int index = i + 1;
            setSQLValueParam(connection, preparedStatement, object, index, false);
        }
    }

    public static int setSQLValueParam(Connection connection, PreparedStatement preparedStatement,
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

    private static int getSQLType(BObject typedValue) throws ApplicationError {
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
                throw new ApplicationError("Unsupported SQL type: " + sqlType);
        }
        return sqlTypeValue;
    }

    private static void setSqlTypedParam(Connection connection, PreparedStatement preparedStatement, int index,
                                         BObject typedValue)
            throws SQLException, ApplicationError, IOException {
        String sqlType = typedValue.getType().getName();
        Object value = typedValue.get(Constants.TypedValueFields.VALUE);
        switch (sqlType) {
            case Constants.SqlTypes.VARCHAR:
            case Constants.SqlTypes.CHAR:
            case Constants.SqlTypes.TEXT:
                if (value == null) {
                    preparedStatement.setString(index, null);
                } else {
                    preparedStatement.setString(index, value.toString());
                }
                break;
            case Constants.SqlTypes.NCHAR:
            case Constants.SqlTypes.NVARCHAR:
                if (value == null) {
                    preparedStatement.setNString(index, null);
                } else {
                    preparedStatement.setNString(index, value.toString());
                }
                break;
            case Constants.SqlTypes.BIT:
            case Constants.SqlTypes.BOOLEAN:
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
                break;
            case Constants.SqlTypes.INTEGER:
                if (value == null) {
                    preparedStatement.setNull(index, Types.INTEGER);
                } else if (value instanceof Integer || value instanceof Long) {
                    preparedStatement.setInt(index, ((Number) value).intValue());
                } else {
                    throw throwInvalidParameterError(value, sqlType);
                }
                break;
            case Constants.SqlTypes.BIGINT:
                if (value == null) {
                    preparedStatement.setNull(index, Types.BIGINT);
                } else if (value instanceof Integer || value instanceof Long) {
                    preparedStatement.setLong(index, ((Number) value).longValue());
                } else {
                    throw throwInvalidParameterError(value, sqlType);
                }
                break;
            case Constants.SqlTypes.SMALLINT:
                if (value == null) {
                    preparedStatement.setNull(index, Types.SMALLINT);
                } else if (value instanceof Integer || value instanceof Long) {
                    preparedStatement.setShort(index, ((Number) value).shortValue());
                } else {
                    throw throwInvalidParameterError(value, sqlType);
                }
                break;
            case Constants.SqlTypes.FLOAT:
            case Constants.SqlTypes.REAL:
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
                break;
            case Constants.SqlTypes.DOUBLE:
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
                break;
            case Constants.SqlTypes.NUMERIC:
            case Constants.SqlTypes.DECIMAL:
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
                break;
            case Constants.SqlTypes.BINARY:
            case Constants.SqlTypes.VARBINARY:
            case Constants.SqlTypes.BLOB:
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
                break;
            case Constants.SqlTypes.CLOB:
            case Constants.SqlTypes.NCLOB:
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
                break;
            case Constants.SqlTypes.DATE:
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
                break;
            case Constants.SqlTypes.TIME:
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
                break;
            case Constants.SqlTypes.TIMESTAMP:
            case Constants.SqlTypes.DATETIME:
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
                break;
            case Constants.SqlTypes.ARRAY:
                Object[] arrayData = getArrayData(value);
                if (arrayData[0] != null) {
                    Array array = connection.createArrayOf((String) arrayData[1], (Object[]) arrayData[0]);
                    preparedStatement.setArray(index, array);
                } else {
                    preparedStatement.setArray(index, null);
                }
                break;
            case Constants.SqlTypes.REF:
            case Constants.SqlTypes.STRUCT:
                Object[] structData = getStructData(value, connection);
                Object[] dataArray = (Object[]) structData[0];
                String structuredSQLType = (String) structData[1];
                if (dataArray == null) {
                    preparedStatement.setNull(index, Types.STRUCT);
                } else {
                    Struct struct = connection.createStruct(structuredSQLType, dataArray);
                    preparedStatement.setObject(index, struct);
                }
                break;
            case Constants.SqlTypes.ROW:
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
                break;
            default:
                throw new ApplicationError("Unsupported SQL type: " + sqlType);
        }
    }

    private static Object[] getArrayData(Object value) throws ApplicationError {
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
                arrayLength = ((BArray) value).size();
                arrayData = new Long[arrayLength];
                for (int i = 0; i < arrayLength; i++) {
                    arrayData[i] = ((BArray) value).getInt(i);
                }
                return new Object[]{arrayData, "BIGINT"};
            case TypeTags.FLOAT_TAG:
                arrayLength = ((BArray) value).size();
                arrayData = new Double[arrayLength];
                for (int i = 0; i < arrayLength; i++) {
                    arrayData[i] = ((BArray) value).getFloat(i);
                }
                return new Object[]{arrayData, "DOUBLE"};
            case TypeTags.DECIMAL_TAG:
                arrayLength = ((BArray) value).size();
                arrayData = new BigDecimal[arrayLength];
                for (int i = 0; i < arrayLength; i++) {
                    arrayData[i] = ((BDecimal) ((BArray) value).getRefValue(i)).value();
                }
                return new Object[]{arrayData, "DECIMAL"};
            case TypeTags.STRING_TAG:
                arrayLength = ((BArray) value).size();
                arrayData = new String[arrayLength];
                for (int i = 0; i < arrayLength; i++) {
                    arrayData[i] = ((BArray) value).getBString(i).getValue();
                }
                return new Object[]{arrayData, "VARCHAR"};
            case TypeTags.BOOLEAN_TAG:
                arrayLength = ((BArray) value).size();
                arrayData = new Boolean[arrayLength];
                for (int i = 0; i < arrayLength; i++) {
                    arrayData[i] = ((BArray) value).getBoolean(i);
                }
                return new Object[]{arrayData, "BOOLEAN"};
            case TypeTags.ARRAY_TAG:
                Type elementTypeOfArrayElement = ((ArrayType) elementType)
                        .getElementType();
                if (elementTypeOfArrayElement.getTag() == TypeTags.BYTE_TAG) {
                    BArray arrayValue = (BArray) value;
                    arrayData = new byte[arrayValue.size()][];
                    for (int i = 0; i < arrayData.length; i++) {
                        arrayData[i] = ((BArray) arrayValue.get(i)).getBytes();
                    }
                    return new Object[]{arrayData, "BINARY"};
                } else {
                    throw throwInvalidParameterError(value, Constants.SqlTypes.ARRAY);
                }
            default:
                throw throwInvalidParameterError(value, Constants.SqlTypes.ARRAY);
        }
    }

    private static Object[] getStructData(Object value, Connection conn) throws SQLException, ApplicationError {
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
                    Type elementType = ((ArrayType) field
                            .getFieldType()).getElementType();
                    if (elementType.getTag() == TypeTags.BYTE_TAG) {
                        structData[i] = ((BArray) bValue).getBytes();
                        break;
                    } else {
                        throw new ApplicationError("unsupported data type of " + structuredSQLType
                                + " specified for struct parameter");
                    }
                case TypeTags.RECORD_TYPE_TAG:
                    Object structValue = bValue;
                    Object[] internalStructData = getStructData(structValue, conn);
                    Object[] dataArray = (Object[]) internalStructData[0];
                    String internalStructType = (String) internalStructData[1];
                    structValue = conn.createStruct(internalStructType, dataArray);
                    structData[i] = structValue;
                    break;
                default:
                    throw new ApplicationError("unsupported data type of " + structuredSQLType
                            + " specified for struct parameter");
            }
        }
        return new Object[]{structData, structuredSQLType};
    }

    private static ApplicationError throwInvalidParameterError(Object value, String sqlType) {
        String valueName;
        if (value instanceof BValue) {
            valueName = ((BValue) value).getType().getName();
        } else {
            valueName = value.getClass().getName();
        }
        return new ApplicationError("Invalid parameter :" + valueName + " is passed as value for SQL type : "
                + sqlType);
    }


    static BArray convert(Array array, int sqlType, Type type) throws SQLException, ApplicationError {
        if (array != null) {
            validatedInvalidFieldAssignment(sqlType, type, "SQL Array");
            Object[] dataArray = (Object[]) array.getArray();
            if (dataArray == null || dataArray.length == 0) {
                return null;
            }

            Object[] result = validateNullable(dataArray);
            Object firstNonNullElement = result[0];
            boolean containsNull = (boolean) result[1];

            if (containsNull) {
                // If there are some null elements, return a union-type element array
                return createAndPopulateBBRefValueArray(firstNonNullElement, dataArray, type);
            } else {
                // If there are no null elements, return a ballerina primitive-type array
                return createAndPopulatePrimitiveValueArray(firstNonNullElement, dataArray);
            }

        } else {
            return null;
        }
    }

    private static BArray createAndPopulatePrimitiveValueArray(Object firstNonNullElement, Object[] dataArray) {
        int length = dataArray.length;
        if (firstNonNullElement instanceof String) {
            BArray stringDataArray = ValueCreator.createArrayValue(stringArrayType);
            for (int i = 0; i < length; i++) {
                stringDataArray.add(i, StringUtils.fromString((String) dataArray[i]));
            }
            return stringDataArray;
        } else if (firstNonNullElement instanceof Boolean) {
            BArray boolDataArray = ValueCreator.createArrayValue(booleanArrayType);
            for (int i = 0; i < length; i++) {
                boolDataArray.add(i, ((Boolean) dataArray[i]).booleanValue());
            }
            return boolDataArray;
        } else if (firstNonNullElement instanceof Integer) {
            BArray intDataArray = ValueCreator.createArrayValue(intArrayType);
            for (int i = 0; i < length; i++) {
                intDataArray.add(i, ((Integer) dataArray[i]).intValue());
            }
            return intDataArray;
        } else if (firstNonNullElement instanceof Long) {
            BArray longDataArray = ValueCreator.createArrayValue(intArrayType);
            for (int i = 0; i < length; i++) {
                longDataArray.add(i, ((Long) dataArray[i]).longValue());
            }
            return longDataArray;
        } else if (firstNonNullElement instanceof Float) {
            BArray floatDataArray = ValueCreator.createArrayValue(floatArrayType);
            for (int i = 0; i < length; i++) {
                floatDataArray.add(i, ((Float) dataArray[i]).floatValue());
            }
            return floatDataArray;
        } else if (firstNonNullElement instanceof Double) {
            BArray doubleDataArray = ValueCreator.createArrayValue(floatArrayType);
            for (int i = 0; i < dataArray.length; i++) {
                doubleDataArray.add(i, ((Double) dataArray[i]).doubleValue());
            }
            return doubleDataArray;
        } else if ((firstNonNullElement instanceof BigDecimal)) {
            BArray decimalDataArray = ValueCreator.createArrayValue(decimalArrayType);
            for (int i = 0; i < dataArray.length; i++) {
                decimalDataArray.add(i, ValueCreator.createDecimalValue((BigDecimal) dataArray[i]));
            }
            return decimalDataArray;
        } else {
            return null;
        }
    }

    static String getString(Clob data) throws IOException, SQLException {
        if (data == null) {
            return null;
        }
        try (Reader r = new BufferedReader(data.getCharacterStream())) {
            StringBuilder sb = new StringBuilder();
            int pos;
            while ((pos = r.read()) != -1) {
                sb.append((char) pos);
            }
            return sb.toString();
        }
    }

    private static BArray createAndPopulateBBRefValueArray(Object firstNonNullElement, Object[] dataArray,
                                                           Type type) {
        BArray refValueArray = null;
        int length = dataArray.length;
        if (firstNonNullElement instanceof String) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_STRING);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, dataArray[i]);
            }
        } else if (firstNonNullElement instanceof Boolean) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_BOOLEAN);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, dataArray[i]);
            }
        } else if (firstNonNullElement instanceof Integer) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_INT);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, dataArray[i]);
            }
        } else if (firstNonNullElement instanceof Long) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_INT);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, dataArray[i]);
            }
        } else if (firstNonNullElement instanceof Float) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_FLOAT);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, dataArray[i]);
            }
        } else if (firstNonNullElement instanceof Double) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_FLOAT);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, dataArray[i]);
            }
        } else if (firstNonNullElement instanceof BigDecimal) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_DECIMAL);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i,
                        dataArray[i] != null ? ValueCreator.createDecimalValue((BigDecimal) dataArray[i]) : null);
            }
        } else if (firstNonNullElement == null) {
            refValueArray = createEmptyBBRefValueArray(type);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, firstNonNullElement);
            }
        }
        return refValueArray;
    }

    private static BArray createEmptyBBRefValueArray(Type type) {
        List<Type> memberTypes = new ArrayList<>(2);
        memberTypes.add(type);
        memberTypes.add(PredefinedTypes.TYPE_NULL);
        UnionType unionType = TypeCreator.createUnionType(memberTypes);
        return ValueCreator.createArrayValue(TypeCreator.createArrayType(unionType));
    }

    private static Object[] validateNullable(Object[] objects) {
        Object[] returnResult = new Object[2];
        boolean foundNull = false;
        Object nonNullObject = null;
        for (Object object : objects) {
            if (object != null) {
                if (nonNullObject == null) {
                    nonNullObject = object;
                }
                if (foundNull) {
                    break;
                }
            } else {
                foundNull = true;
                if (nonNullObject != null) {
                    break;
                }
            }
        }
        returnResult[0] = nonNullObject;
        returnResult[1] = foundNull;
        return returnResult;
    }

    static BString convert(String value, int sqlType, Type type) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, "SQL String");
        return fromString(value);
    }

    static Object convert(String value, int sqlType, Type type, String sqlTypeName) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, sqlTypeName);
        return fromString(value);
    }

    static Object convert(byte[] value, int sqlType, Type type, String sqlTypeName) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, sqlTypeName);
        if (value != null) {
            return ValueCreator.createArrayValue(value);
        } else {
            return null;
        }
    }

    static Object convert(long value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, "SQL long or integer");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return value;
        }
    }

    static Object convert(double value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, "SQL double or float");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return value;
        }
    }

    static Object convert(BigDecimal value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, "SQL decimal or real");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return ValueCreator.createDecimalValue(value);
        }
    }

    static Object convert(Blob value, int sqlType, Type type) throws ApplicationError, SQLException {
        validatedInvalidFieldAssignment(sqlType, type, "SQL Blob");
        if (value != null) {
            return ValueCreator.createArrayValue(value.getBytes(1L, (int) value.length()));
        } else {
            return null;
        }
    }

    static Object convert(java.util.Date date, int sqlType, Type type) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (date != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(getString(date));
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    return createTimeStruct(date.getTime());
                case TypeTags.INT_TAG:
                    return date.getTime();
            }
        }
        return null;
    }

    static Object convert(boolean value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, "SQL Boolean");
        if (!isNull) {
            switch (type.getTag()) {
                case TypeTags.BOOLEAN_TAG:
                    return value;
                case TypeTags.INT_TAG:
                    if (value) {
                        return 1L;
                    } else {
                        return 0L;
                    }
                case TypeTags.STRING_TAG:
                    return fromString(String.valueOf(value));
            }
        }
        return null;
    }

    static Object convert(Struct value, int sqlType, Type type) throws ApplicationError {
        validatedInvalidFieldAssignment(sqlType, type, "SQL Struct");
        if (value != null) {
            if (type instanceof RecordType) {
                return createUserDefinedType(value, (RecordType) type);
            } else {
                throw new ApplicationError("The ballerina type that can be used for SQL struct should be record type," +
                        " but found " + type.getName() + " .");
            }
        } else {
            return null;
        }
    }

    static Object convert(SQLXML value, int sqlType, Type type) throws ApplicationError, SQLException {
        validatedInvalidFieldAssignment(sqlType, type, "SQL XML");
        if (value != null) {
            if (type instanceof BXml) {
                return XmlUtils.parse(value.getBinaryStream());
            } else {
                throw new ApplicationError("The ballerina type that can be used for SQL struct should be record type," +
                        " but found " + type.getName() + " .");
            }
        } else {
            return null;
        }
    }

    private static BMap<BString, Object> createUserDefinedType(Struct structValue, StructureType structType)
            throws ApplicationError {
        if (structValue == null) {
            return null;
        }
        Field[] internalStructFields = structType.getFields().values().toArray(new Field[0]);
        BMap<BString, Object> struct = ValueCreator.createMapValue(structType);
        try {
            Object[] dataArray = structValue.getAttributes();
            if (dataArray != null) {
                if (dataArray.length != internalStructFields.length) {
                    throw new ApplicationError("specified record and the returned SQL Struct field counts " +
                            "are different, and hence not compatible");
                }
                int index = 0;
                for (Field internalField : internalStructFields) {
                    int type = internalField.getFieldType().getTag();
                    BString fieldName = fromString(internalField.getFieldName());
                    Object value = dataArray[index];
                    switch (type) {
                        case TypeTags.INT_TAG:
                            if (value instanceof BigDecimal) {
                                struct.put(fieldName, ((BigDecimal) value).intValue());
                            } else {
                                struct.put(fieldName, value);
                            }
                            break;
                        case TypeTags.FLOAT_TAG:
                            if (value instanceof BigDecimal) {
                                struct.put(fieldName, ((BigDecimal) value).doubleValue());
                            } else {
                                struct.put(fieldName, value);
                            }
                            break;
                        case TypeTags.DECIMAL_TAG:
                            if (value instanceof BigDecimal) {
                                struct.put(fieldName, value);
                            } else {
                                struct.put(fieldName, ValueCreator.createDecimalValue((BigDecimal) value));
                            }
                            break;
                        case TypeTags.STRING_TAG:
                            struct.put(fieldName, value);
                            break;
                        case TypeTags.BOOLEAN_TAG:
                            struct.put(fieldName, ((int) value) == 1);
                            break;
                        case TypeTags.OBJECT_TYPE_TAG:
                        case TypeTags.RECORD_TYPE_TAG:
                            struct.put(fieldName,
                                    createUserDefinedType((Struct) value,
                                            (StructureType) internalField.getFieldType()));
                            break;
                        default:
                            throw new ApplicationError("Error while retrieving data for unsupported type " +
                                    internalField.getFieldType().getName() + " to create "
                                    + structType.getName() + " record.");
                    }
                    ++index;
                }
            }
        } catch (SQLException e) {
            throw new ApplicationError("Error while retrieving data to create " + structType.getName()
                    + " record. ", e);
        }
        return struct;
    }


    private static BMap<BString, Object> createTimeStruct(long millis) {
        return TimeUtils.createTimeRecord(millis, Constants.TIMEZONE_UTC);
    }

    private static String getString(java.util.Date value) {
        if (value == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(value);

        if (!calendar.isSet(Calendar.ZONE_OFFSET)) {
            calendar.setTimeZone(TimeZone.getDefault());
        }
        StringBuffer datetimeString = new StringBuffer(28);
        if (value instanceof Date) {
            //'-'? yyyy '-' mm '-' dd zzzzzz?
            calendar.setTime(value);
            appendDate(datetimeString, calendar);
            appendTimeZone(calendar, datetimeString);
        } else if (value instanceof Time) {
            //hh ':' mm ':' ss ('.' s+)? (zzzzzz)?
            calendar.setTimeInMillis(value.getTime());
            appendTime(calendar, datetimeString);
            appendTimeZone(calendar, datetimeString);
        } else if (value instanceof Timestamp) {
            calendar.setTimeInMillis(value.getTime());
            appendDate(datetimeString, calendar);
            datetimeString.append("T");
            appendTime(calendar, datetimeString);
            appendTimeZone(calendar, datetimeString);
        } else {
            calendar.setTime(value);
            appendTime(calendar, datetimeString);
            appendTimeZone(calendar, datetimeString);
        }
        return datetimeString.toString();
    }

    private static void appendTimeZone(Calendar calendar, StringBuffer dateString) {
        int timezoneOffSet = calendar.get(Calendar.ZONE_OFFSET) + calendar.get(Calendar.DST_OFFSET);
        int timezoneOffSetInMinits = timezoneOffSet / 60000;
        if (timezoneOffSetInMinits < 0) {
            dateString.append("-");
            timezoneOffSetInMinits = timezoneOffSetInMinits * -1;
        } else {
            dateString.append("+");
        }
        int hours = timezoneOffSetInMinits / 60;
        int minits = timezoneOffSetInMinits % 60;
        if (hours < 10) {
            dateString.append("0");
        }
        dateString.append(hours).append(":");
        if (minits < 10) {
            dateString.append("0");
        }
        dateString.append(minits);
    }

    private static void appendTime(Calendar value, StringBuffer dateString) {
        if (value.get(Calendar.HOUR_OF_DAY) < 10) {
            dateString.append("0");
        }
        dateString.append(value.get(Calendar.HOUR_OF_DAY)).append(":");
        if (value.get(Calendar.MINUTE) < 10) {
            dateString.append("0");
        }
        dateString.append(value.get(Calendar.MINUTE)).append(":");
        if (value.get(Calendar.SECOND) < 10) {
            dateString.append("0");
        }
        dateString.append(value.get(Calendar.SECOND)).append(".");
        if (value.get(Calendar.MILLISECOND) < 10) {
            dateString.append("0");
        }
        if (value.get(Calendar.MILLISECOND) < 100) {
            dateString.append("0");
        }
        dateString.append(value.get(Calendar.MILLISECOND));
    }

    private static void appendDate(StringBuffer dateString, Calendar calendar) {
        int year = calendar.get(Calendar.YEAR);
        if (year < 1000) {
            dateString.append("0");
        }
        if (year < 100) {
            dateString.append("0");
        }
        if (year < 10) {
            dateString.append("0");
        }
        dateString.append(year).append("-");
        // sql date month is started from 1 and calendar month is
        // started from 0. so have to add one
        int month = calendar.get(Calendar.MONTH) + 1;
        if (month < 10) {
            dateString.append("0");
        }
        dateString.append(month).append("-");
        if (calendar.get(Calendar.DAY_OF_MONTH) < 10) {
            dateString.append("0");
        }
        dateString.append(calendar.get(Calendar.DAY_OF_MONTH));
    }

    private static void validatedInvalidFieldAssignment(int sqlType, Type type, String sqlTypeName)
            throws ApplicationError {
        if (!isValidFieldConstraint(sqlType, type)) {
            throw new ApplicationError(sqlTypeName + " field cannot be converted to ballerina type : "
                    + type.getName());
        }
    }

    private static Type validFieldConstraint(int sqlType, Type type) {
        if (type.getTag() == TypeTags.UNION_TAG && type instanceof UnionType) {
            UnionType bUnionType = (UnionType) type;
            for (Type memberType : bUnionType.getMemberTypes()) {
                //In case if the member type is another union type, check recursively.
                if (isValidFieldConstraint(sqlType, memberType)) {
                    return memberType;
                }
            }
        } else {
            if (isValidPrimitiveConstraint(sqlType, type)) {
                return type;
            }
        }
        return null;
    }

    public static boolean isValidFieldConstraint(int sqlType, Type type) {
        if (type.getTag() == TypeTags.UNION_TAG && type instanceof UnionType) {
            UnionType bUnionType = (UnionType) type;
            for (Type memberType : bUnionType.getMemberTypes()) {
                //In case if the member type is another union type, check recursively.
                if (isValidFieldConstraint(sqlType, memberType)) {
                    return true;
                }
            }
            return false;
        } else {
            return isValidPrimitiveConstraint(sqlType, type);
        }
    }

    private static boolean isValidPrimitiveConstraint(int sqlType, Type type) {
        switch (sqlType) {
            case Types.ARRAY:
                return type.getTag() == TypeTags.ARRAY_TAG;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return type.getTag() == TypeTags.STRING_TAG ||
                        type.getTag() == TypeTags.JSON_TAG;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.TIME_WITH_TIMEZONE:
                return type.getTag() == TypeTags.STRING_TAG ||
                        type.getTag() == TypeTags.OBJECT_TYPE_TAG ||
                        type.getTag() == TypeTags.RECORD_TYPE_TAG ||
                        type.getTag() == TypeTags.INT_TAG;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return type.getTag() == TypeTags.INT_TAG ||
                        type.getTag() == TypeTags.STRING_TAG;
            case Types.BIT:
            case Types.BOOLEAN:
                return type.getTag() == TypeTags.BOOLEAN_TAG ||
                        type.getTag() == TypeTags.INT_TAG ||
                        type.getTag() == TypeTags.STRING_TAG;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return type.getTag() == TypeTags.DECIMAL_TAG ||
                        type.getTag() == TypeTags.INT_TAG ||
                        type.getTag() == TypeTags.STRING_TAG;
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return type.getTag() == TypeTags.FLOAT_TAG ||
                        type.getTag() == TypeTags.STRING_TAG;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.ROWID:
                if (type.getTag() == TypeTags.ARRAY_TAG) {
                    int elementTypeTag = ((ArrayType) type).getElementType().getTag();
                    return elementTypeTag == TypeTags.BYTE_TAG;
                }
                return type.getTag() == TypeTags.STRING_TAG || type.getTag() == TypeTags.BYTE_ARRAY_TAG;
            case Types.REF:
            case Types.STRUCT:
                return type.getTag() == TypeTags.RECORD_TYPE_TAG;
            case Types.SQLXML:
                return type.getTag() == TypeTags.XML_TAG;
            default:
                //If user is passing the intended type variable for the sql types, then it will use
                // those types to resolve the result.
                return type.getTag() == TypeTags.ANY_TAG ||
                        type.getTag() == TypeTags.ANYDATA_TAG ||
                        (type.getTag() == TypeTags.ARRAY_TAG &&
                                ((ArrayType) type).getElementType().getTag() == TypeTags.BYTE_TAG) ||
                        type.getTag() == TypeTags.STRING_TAG ||
                        type.getTag() == TypeTags.INT_TAG ||
                        type.getTag() == TypeTags.BOOLEAN_TAG ||
                        type.getTag() == TypeTags.XML_TAG ||
                        type.getTag() == TypeTags.FLOAT_TAG ||
                        type.getTag() == TypeTags.DECIMAL_TAG ||
                        type.getTag() == TypeTags.JSON_TAG;
        }
    }

    public static Object getGeneratedKeys(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        if (columnCount > 0) {
            int sqlType = metaData.getColumnType(1);
            switch (sqlType) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.BIT:
                case Types.BOOLEAN:
                    return rs.getLong(1);
                default:
                    return rs.getString(1);
            }
        }
        return null;
    }

    public static BObject createRecordIterator(ResultSet resultSet,
                                               Statement statement,
                                               Connection connection, List<ColumnDefinition> columnDefinitions,
                                               StructureType streamConstraint) {
        BObject resultIterator = ValueCreator.createObjectValue(ModuleUtils.getModule(),
                Constants.RESULT_ITERATOR_OBJECT, new Object[1]);
        resultIterator.addNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD, resultSet);
        resultIterator.addNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD, statement);
        resultIterator.addNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD, connection);
        resultIterator.addNativeData(Constants.COLUMN_DEFINITIONS_DATA_FIELD, columnDefinitions);
        resultIterator.addNativeData(Constants.RECORD_TYPE_DATA_FIELD, streamConstraint);
        return resultIterator;
    }

    public static StructureType getDefaultRecordType(List<ColumnDefinition> columnDefinitions) {
        RecordType defaultRecord = getDefaultStreamConstraint();
        Map<String, Field> fieldMap = new HashMap<>();
        for (ColumnDefinition column : columnDefinitions) {
            long flags = SymbolFlags.PUBLIC;
            if (column.isNullable()) {
                flags += SymbolFlags.OPTIONAL;
            } else {
                flags += SymbolFlags.REQUIRED;
            }
            fieldMap.put(column.getColumnName(), TypeCreator.createField(column.getBallerinaType(),
                    column.getColumnName(), flags));
        }
        defaultRecord.setFields(fieldMap);
        return defaultRecord;
    }

    public static RecordType getDefaultStreamConstraint() {
        Module ballerinaAnnotation = new Module("ballerina", "lang.annotations", "0.0.0");
        return TypeCreator.createRecordType(
                "$stream$anon$constraint$", ballerinaAnnotation, 0,
                new HashMap<>(), PredefinedTypes.TYPE_ANYDATA, false,
                TypeFlags.asMask(TypeFlags.ANYDATA, TypeFlags.PURETYPE));
    }

    public static List<ColumnDefinition> getColumnDefinitions(ResultSet resultSet, StructureType streamConstraint)
            throws SQLException, ApplicationError {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        Set<String> columnNames = new HashSet<>();
        ResultSetMetaData rsMetaData = resultSet.getMetaData();
        int cols = rsMetaData.getColumnCount();
        for (int i = 1; i <= cols; i++) {
            String colName = rsMetaData.getColumnLabel(i);
            if (columnNames.contains(colName)) {
                String tableName = rsMetaData.getTableName(i).toUpperCase(Locale.getDefault());
                colName = tableName + "." + colName;
            }
            int sqlType = rsMetaData.getColumnType(i);
            String sqlTypeName = rsMetaData.getColumnTypeName(i);
            boolean isNullable = true;
            if (rsMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                isNullable = false;
            }
            columnDefs.add(generateColumnDefinition(colName, sqlType, sqlTypeName, streamConstraint, isNullable));
            columnNames.add(colName);
        }
        return columnDefs;
    }

    private static ColumnDefinition generateColumnDefinition(String columnName, int sqlType, String sqlTypeName,
                                                             StructureType streamConstraint, boolean isNullable)
            throws ApplicationError {
        String ballerinaFieldName = null;
        Type ballerinaType = null;
        if (streamConstraint != null) {
            for (Map.Entry<String, Field> field : streamConstraint.getFields().entrySet()) {
                if (field.getKey().equalsIgnoreCase(columnName)) {
                    ballerinaFieldName = field.getKey();
                    ballerinaType = validFieldConstraint(sqlType, field.getValue().getFieldType());
                    if (ballerinaType == null) {
                        throw new ApplicationError(
                                field.getValue().getFieldType().getName() + " cannot be mapped to SQL type '"
                                        + sqlTypeName + "'");
                    }
                    break;
                }
            }
            if (ballerinaFieldName == null) {
                throw new ApplicationError("No mapping field found for SQL table column '" + columnName + "'"
                        + " in the record type '" + streamConstraint.getName() + "'");
            }
        } else {
            ballerinaType = getDefaultBallerinaType(sqlType);
            ballerinaFieldName = columnName;
        }
        return new ColumnDefinition(columnName, ballerinaFieldName, sqlType, sqlTypeName, ballerinaType, isNullable);

    }

    private static Type getDefaultBallerinaType(int sqlType) {
        switch (sqlType) {
            case Types.ARRAY:
                return TypeCreator.createArrayType(PredefinedTypes.TYPE_ANYDATA);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.TIME_WITH_TIMEZONE:
                return PredefinedTypes.TYPE_STRING;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return PredefinedTypes.TYPE_INT;
            case Types.BIT:
            case Types.BOOLEAN:
                return PredefinedTypes.TYPE_BOOLEAN;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return PredefinedTypes.TYPE_DECIMAL;
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return PredefinedTypes.TYPE_FLOAT;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.ROWID:
                return TypeCreator.createArrayType(PredefinedTypes.TYPE_BYTE);
            case Types.REF:
            case Types.STRUCT:
                return getDefaultStreamConstraint();
            case Types.SQLXML:
                return PredefinedTypes.TYPE_XML;
            default:
                return PredefinedTypes.TYPE_ANYDATA;
        }
    }

    public static Object cleanUpConnection(BObject ballerinaObject, ResultSet resultSet,
                                           Statement statement, Connection connection) {
        if (resultSet != null) {
            try {
                resultSet.close();
                ballerinaObject.addNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD, null);
            } catch (SQLException e) {
                return ErrorGenerator.getSQLDatabaseError(e, "Error while closing the result set. ");
            }
        }
        if (statement != null) {
            try {
                statement.close();
                ballerinaObject.addNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD, null);
            } catch (SQLException e) {
                return ErrorGenerator.getSQLDatabaseError(e, "Error while closing the result set. ");
            }
        }
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (!trxResourceManager.isInTransaction() ||
                !trxResourceManager.getCurrentTransactionContext().hasTransactionBlock()) {
            if (connection != null) {
                try {
                    connection.close();
                    ballerinaObject.addNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD, null);
                } catch (SQLException e) {
                    return ErrorGenerator.getSQLDatabaseError(e, "Error while closing the connection. ");
                }
            }
        }
        return null;
    }

    public static void updateProcedureCallExecutionResult(CallableStatement statement, BObject procedureCallResult)
            throws SQLException {
        Object lastInsertedId = null;
        int count = statement.getUpdateCount();
        ResultSet resultSet = statement.getGeneratedKeys();
        if (resultSet.next()) {
            lastInsertedId = getGeneratedKeys(resultSet);
        }
        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put(AFFECTED_ROW_COUNT_FIELD, count);
        resultFields.put(LAST_INSERTED_ID_FIELD, lastInsertedId);
        BMap<BString, Object> executionResult = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), EXECUTION_RESULT_RECORD, resultFields);
        procedureCallResult.set(EXECUTION_RESULT_FIELD, executionResult);
    }
}
