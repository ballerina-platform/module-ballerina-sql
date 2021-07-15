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
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BXml;
import io.ballerina.stdlib.io.channels.base.Channel;
import io.ballerina.stdlib.io.channels.base.CharacterChannel;
import io.ballerina.stdlib.io.readers.CharacterChannelReader;
import io.ballerina.stdlib.io.utils.IOConstants;
import io.ballerina.stdlib.io.utils.IOUtils;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.utils.Utils;
import io.ballerina.stdlib.time.util.TimeValueHandler;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;

/**
 * Represent the Process methods for statements.
 *
 * @since 0.5.6
 */
public class DefaultStatementParameterProcessor extends AbstractStatementParameterProcessor {

    private static final Object lock = new Object();
    private static volatile DefaultStatementParameterProcessor instance;

    public DefaultStatementParameterProcessor() {
    }

    public static DefaultStatementParameterProcessor getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new DefaultStatementParameterProcessor();
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
            case Constants.SqlTypes.TIME_OF_DAY_ARRAY:
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

    private void setSqlTypedParam(Connection connection, PreparedStatement preparedStatement, int index,
                                    BObject typedValue)
            throws SQLException, ApplicationError, IOException {
        String sqlType = typedValue.getType().getName();
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

    private Object[] getArrayData(Object value) throws ApplicationError {
        Type type = TypeUtils.getType(value);
        if (value == null || type.getTag() != TypeTags.ARRAY_TAG) {
            return new Object[]{null, null};
        }
        Type elementType = ((ArrayType) type).getElementType();
        int typeTag = elementType.getTag();
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

    protected Object[] getIntValueArrayData(Object value, String type, String dataType) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Short[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof Integer || innerValue instanceof Long) {
                arrayData[i] = ((Number) innerValue).shortValue();
            } else {
                throw Utils.throwInvalidParameterError(innerValue, dataType);
            }
        }        
        return new Object[]{arrayData, type};
    }

    protected Object[] getDecimalValueArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new BigDecimal[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof Double || innerValue instanceof Long) {
                arrayData[i] = new BigDecimal(((Number) innerValue).doubleValue(), MathContext.DECIMAL64);
            } else if (innerValue instanceof Integer || innerValue instanceof Float) {
                arrayData[i] = new BigDecimal(((Number) innerValue).doubleValue(), MathContext.DECIMAL32);
            } else if (innerValue instanceof BDecimal) {
                arrayData[i] = ((BDecimal) innerValue).decimalValue();
            } else {
                throw Utils.throwInvalidParameterError(innerValue, "Decimal Array");
            }            
        }        
        return new Object[]{arrayData, Constants.SqlArrays.DECIMAL};
    }
    
    protected Object[] getRealValueArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Double[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof Double) {
                arrayData[i] = ((Number) innerValue).doubleValue();
            } else if (innerValue instanceof Long || innerValue instanceof Float || innerValue instanceof Integer) {
                arrayData[i] = ((Number) innerValue).doubleValue();
            } else if (innerValue instanceof BDecimal) {
                arrayData[i] = ((BDecimal) innerValue).decimalValue().doubleValue();
            } else {
                throw Utils.throwInvalidParameterError(innerValue, "Real Array");
            }            
        }        
        return new Object[]{arrayData, Constants.SqlArrays.REAL};
    }

    protected Object[] getNumericValueArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new BigDecimal[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof Double || innerValue instanceof Long) {
                arrayData[i] = new BigDecimal(((Number) innerValue).doubleValue(), MathContext.DECIMAL64);
            } else if (innerValue instanceof Integer || innerValue instanceof Float) {
                arrayData[i] = new BigDecimal(((Number) innerValue).doubleValue(), MathContext.DECIMAL32);
            } else if (innerValue instanceof BDecimal) {
                arrayData[i] = ((BDecimal) innerValue).decimalValue();
            } else {
                throw Utils.throwInvalidParameterError(innerValue, "Numeric Array");
            }            
        }        
        return new Object[]{arrayData, Constants.SqlArrays.NUMERIC};
    }
    
    protected Object[] getDoubleValueArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Double[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof Double) {
                arrayData[i] = ((Number) innerValue).doubleValue();
            } else if (innerValue instanceof Long || innerValue instanceof Float || innerValue instanceof Integer) {
                arrayData[i] = ((Number) innerValue).doubleValue();
            } else if (innerValue instanceof BDecimal) {
                arrayData[i] = ((BDecimal) innerValue).decimalValue().doubleValue();
            } else {
                throw Utils.throwInvalidParameterError(innerValue, "Double Array");
            }            
        }        
        return new Object[]{arrayData, Constants.SqlArrays.DOUBLE};
    }

    protected Object[] getFloatValueArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Double[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof Double) {
                arrayData[i] = ((Number) innerValue).doubleValue();
            } else if (innerValue instanceof Long || innerValue instanceof Float || innerValue instanceof Integer) {
                arrayData[i] = ((Number) innerValue).doubleValue();
            } else if (innerValue instanceof BDecimal) {
                arrayData[i] = ((BDecimal) innerValue).decimalValue().doubleValue();
            } else {
                throw Utils.throwInvalidParameterError(innerValue, "Float Array");
            }            
        }        
        return new Object[]{arrayData, Constants.SqlArrays.FLOAT};
    }

    protected Object[] getCharValueArrayData(Object value) throws ApplicationError {
        return getStringValueArrayData(value, Constants.SqlArrays.CHAR);
    }

    protected Object[] getVarcharValueArrayData(Object value) throws ApplicationError {
        return getStringValueArrayData(value, Constants.SqlArrays.VARCHAR);
    }

    protected Object[] getNvarcharValueArrayData(Object value) throws ApplicationError {
        return getStringValueArrayData(value, Constants.SqlArrays.NVARCHAR);
    }

    protected Object[] getDateTimeValueArrayData(Object value) throws ApplicationError {
        return getDateTimeAndTimestampValueArrayData(value);
    }

    protected Object[] getTimestampValueArrayData(Object value) throws ApplicationError {
        return getDateTimeAndTimestampValueArrayData(value);
    }

    protected Object[] getBooleanValueArrayData(Object value) throws ApplicationError {
        return getBitAndBooleanValueArrayData(value, Constants.SqlArrays.BOOLEAN);
    }

    protected Object[] getDateValueArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Date[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof BString) {
                try {
                    arrayData[i] = Date.valueOf(innerValue.toString());
                } catch (java.lang.IllegalArgumentException ex) {
                    throw new ApplicationError("Unsupported String Value " + innerValue
                            .toString() + " for Date Array");
                }
            } else if (innerValue instanceof BMap) {
                BMap dateMap = (BMap) innerValue;
                int year = Math.toIntExact(dateMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_YEAR)));
                int month = Math.toIntExact(dateMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_MONTH)));
                int day = Math.toIntExact(dateMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_DAY)));
                arrayData[i] = Date.valueOf(year + "-" + month + "-" + day);
            } else {
                throw Utils.throwInvalidParameterError(innerValue, "Date Array");
            }            
        }        
        return new Object[]{arrayData, Constants.SqlArrays.DATE};
    }

    protected Object[] getTimeValueArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Object[arrayLength];
        boolean containsTimeZone = false;
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof BString) {
                try {
                    arrayData[i] = Time.valueOf(innerValue.toString());
                } catch (java.lang.NumberFormatException ex) {
                    throw new ApplicationError("Unsupported String Value " + innerValue
                            .toString() + " for Time Array");
                }
                // arrayData[i] = innerValue.toString();
            } else if (innerValue instanceof BMap) {
                BMap timeMap = (BMap) innerValue;
                int hour = Math.toIntExact(timeMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_HOUR)));
                int minute = Math.toIntExact(timeMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_MINUTE)));
                BDecimal second = BDecimal.valueOf(0);
                if (timeMap.containsKey(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_SECOND))) {
                    second = ((BDecimal) timeMap.get(StringUtils
                            .fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_SECOND)));
                }
                int zoneHours = 0;
                int zoneMinutes = 0;
                BDecimal zoneSeconds = BDecimal.valueOf(0);
                boolean timeZone = false;
                if (timeMap.containsKey(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET))) {
                    timeZone = true;
                    containsTimeZone = true;
                    BMap zoneMap = (BMap) timeMap.get(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET));
                    zoneHours = Math.toIntExact(zoneMap.getIntValue(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)));
                    zoneMinutes = Math.toIntExact(zoneMap.getIntValue(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)));
                    if (zoneMap.containsKey(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND))) {
                        zoneSeconds = ((BDecimal) zoneMap.get(StringUtils.
                                fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)));
                    }
                }
                int intSecond = second.decimalValue().setScale(0, RoundingMode.FLOOR).intValue();
                int intNanoSecond = second.decimalValue().subtract(new BigDecimal(intSecond))
                        .multiply(io.ballerina.stdlib.time.util.Constants.ANALOG_GIGA)
                        .setScale(0, RoundingMode.HALF_UP).intValue();
                LocalTime localTime = LocalTime.of(hour, minute, intSecond, intNanoSecond);
                if (timeZone) {
                    int intZoneSecond = zoneSeconds.decimalValue().setScale(0, RoundingMode.HALF_UP)
                            .intValue();
                    OffsetTime offsetTime = OffsetTime.of(localTime,
                            ZoneOffset.ofHoursMinutesSeconds(zoneHours, zoneMinutes, intZoneSecond));
                    arrayData[i] = offsetTime;
                } else {
                    arrayData[i] = Time.valueOf(localTime);
                }
            } else {
                throw Utils.throwInvalidParameterError(innerValue, "Time Array");
            }            
        }        
        if (containsTimeZone) {
            return new Object[]{arrayData, Constants.SqlArrays.TIME_WITH_TIMEZONE};
        } else {
            return new Object[]{arrayData, Constants.SqlArrays.TIME};
        }
    }

    protected Object[] getBinaryValueArrayData(Object value) throws ApplicationError, IOException {
        return getBinaryAndBlobValueArrayData(value, Constants.SqlArrays.BINARY);
    }

    protected Object[] getVarbinaryValueArrayData(Object value) throws ApplicationError, IOException {        
        return getBinaryAndBlobValueArrayData(value, Constants.SqlArrays.VARBINARY);
    }

    protected Object[] getBitValueArrayData(Object value) throws ApplicationError {
        return getBitAndBooleanValueArrayData(value, Constants.SqlArrays.BIT);
    }

    private Object[] getBitAndBooleanValueArrayData(Object value, String type) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Boolean[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof BString) {
                arrayData[i] = Boolean.parseBoolean(innerValue.toString());
            } else if (innerValue instanceof Integer || innerValue instanceof Long) {
                long lVal = ((Number) innerValue).longValue();
                if (lVal == 1 || lVal == 0) {
                    arrayData[i] = lVal == 1;
                } else {
                    throw new ApplicationError("Only 1 or 0 can be passed for " + type
                            + " SQL Type, but found :" + lVal);
                }
            } else if (innerValue instanceof Boolean) {
                arrayData[i] = (Boolean) innerValue;
            } else {
                throw Utils.throwInvalidParameterError(innerValue, type + " Array");
            }            
        }        
        return new Object[]{arrayData, type};
    }

    private Object[] getBinaryAndBlobValueArrayData(Object value, String type) 
            throws ApplicationError, IOException {     
        BObject objectValue;
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Object[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof BArray) {                
                BArray arrayValue = (BArray) innerValue;
                if (arrayValue.getElementType().getTag() == org.wso2.ballerinalang.compiler.util.TypeTags.BYTE) {
                    arrayData[i] = arrayValue.getBytes();
                } else {
                    throw Utils.throwInvalidParameterError(innerValue, type);
                }
            } else if (innerValue instanceof BObject) {                
                objectValue = (BObject) innerValue;
                if (objectValue.getType().getName().equalsIgnoreCase(Constants.READ_BYTE_CHANNEL_STRUCT) &&
                        objectValue.getType().getPackage().toString()
                            .equalsIgnoreCase(IOUtils.getIOPackage().toString())) {
                    Channel byteChannel = (Channel) objectValue.getNativeData(IOConstants.BYTE_CHANNEL_NAME);
                    arrayData[i] = toByteArray(byteChannel.getInputStream());
                } else {
                    throw Utils.throwInvalidParameterError(innerValue, type + " Array");
                }
            } else {
                throw Utils.throwInvalidParameterError(innerValue, type);
            }            
        }        
        return new Object[]{arrayData, type};
    }

    public static byte[] toByteArray(java.io.InputStream in) throws IOException {
        java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = in.read(buffer)) != -1) {
            os.write(buffer, 0, len);
        }
        return os.toByteArray();
    }

    private Object[] getDateTimeAndTimestampValueArrayData(Object value) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        boolean containsTimeZone = false;
        BArray array = ((BArray) value);
        Object[] arrayData = new Object[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            if (innerValue == null) {
                arrayData[i] = null;
            } else if (innerValue instanceof BString) {
                try {
                    java.time.format.DateTimeFormatter formatter = 
                    java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    arrayData[i] = LocalDateTime.parse(innerValue.toString(), formatter);
                } catch (java.time.format.DateTimeParseException ex) {
                    throw new ApplicationError("Unsupported String Value " + innerValue
                            .toString() + " for DateTime Array");
                }
            } else if (innerValue instanceof BArray) {
                //this is mapped to time:Utc
                BArray dateTimeStruct = (BArray) innerValue;
                ZonedDateTime zonedDt = TimeValueHandler.createZonedDateTimeFromUtc(dateTimeStruct);
                Timestamp timestamp = new Timestamp(zonedDt.toInstant().toEpochMilli());
                arrayData[i] = timestamp;
            } else if (innerValue instanceof BMap) {
                //this is mapped to time:Civil
                BMap dateMap = (BMap) innerValue;
                int year = Math.toIntExact(dateMap.getIntValue(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_YEAR)));
                int month = Math.toIntExact(dateMap.getIntValue(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_MONTH)));
                int day = Math.toIntExact(dateMap.getIntValue(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_DAY)));
                int hour = Math.toIntExact(dateMap.getIntValue(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_HOUR)));
                int minute = Math.toIntExact(dateMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_MINUTE)));
                BDecimal second = BDecimal.valueOf(0);
                if (dateMap.containsKey(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_SECOND))) {
                    second = ((BDecimal) dateMap.get(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_SECOND)));
                }
                int zoneHours = 0;
                int zoneMinutes = 0;
                BDecimal zoneSeconds = BDecimal.valueOf(0);
                boolean timeZone = false;
                if (dateMap.containsKey(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET))) {
                    timeZone = true;
                    containsTimeZone = true;
                    BMap zoneMap = (BMap) dateMap.get(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET));
                    zoneHours = Math.toIntExact(zoneMap.getIntValue(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)));
                    zoneMinutes = Math.toIntExact(zoneMap.getIntValue(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)));
                    if (zoneMap.containsKey(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND))) {
                        zoneSeconds = ((BDecimal) zoneMap.get(StringUtils.
                                fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)));
                    }
                }
                int intSecond = second.decimalValue().setScale(0, RoundingMode.FLOOR).intValue();
                int intNanoSecond = second.decimalValue().subtract(new BigDecimal(intSecond))
                        .multiply(io.ballerina.stdlib.time.util.Constants.ANALOG_GIGA)
                        .setScale(0, RoundingMode.HALF_UP).intValue();
                LocalDateTime localDateTime = LocalDateTime
                        .of(year, month, day, hour, minute, intSecond, intNanoSecond);
                if (timeZone) {
                    int intZoneSecond = zoneSeconds.decimalValue().setScale(0, RoundingMode.HALF_UP)
                            .intValue();
                    OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime,
                            ZoneOffset.ofHoursMinutesSeconds(zoneHours, zoneMinutes, intZoneSecond));
                    arrayData[i] =  offsetDateTime;
                } else {
                    arrayData[i] = Timestamp.valueOf(localDateTime);
                }
            } else {
                throw Utils.throwInvalidParameterError(value, "TIMESTAMP ARRAY");
            }            
        }        
        if (containsTimeZone) {
            return new Object[]{arrayData, Constants.SqlArrays.TIMESTAMP_WITH_TIMEZONE};
        } else {
            return new Object[]{arrayData, Constants.SqlArrays.TIMESTAMP};
        }
    }

    private Object[] getStringValueArrayData(Object value, String type) throws ApplicationError {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new String[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            Object arrayValue = array.get(i);
            if (arrayValue == null) {
                arrayData[i] = null;
            } else if (arrayValue instanceof BString) {
                arrayData[i] = arrayValue.toString();
            } else {
                throw Utils.throwInvalidParameterError(value, type + " Array");
            }
        }        
        return new Object[]{arrayData, type};
    }

    private Object[] getStructData(Connection conn, Object value) throws SQLException, ApplicationError {
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
                    getArrayStructData(field, structData, structuredSQLType, i, bValue);
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    getRecordStructData(conn, structData, i, bValue);
                    break;
                default:
                    getCustomStructData(conn, value);
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
            setSQLValueParam(connection, preparedStatement, index, object, false);
        }
    }

    public int setSQLValueParam(Connection connection, PreparedStatement preparedStatement,
                    int index, Object object, boolean returnType) throws SQLException, ApplicationError, IOException {
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
            String type = objectArray.getElementType().toString();
            if (objectArray.getElementType().getTag() == TypeTags.BYTE_TAG) {
                preparedStatement.setBytes(index, objectArray.getBytes());
            } else if (objectArray.getElementType().getTag() == TypeTags.ARRAY_TAG ||
                    type.equals(Constants.SqlTypes.OPTIONAL_BYTE) || type.equals(Constants.SqlTypes.BYTE_ARRAY_TYPE)) {
                setBinaryArray(connection, preparedStatement, index, objectArray);
            } else if (type.equals(Constants.SqlTypes.STRING) || type.equals(Constants.SqlTypes.OPTIONAL_STRING)) {
                setVarcharArray(connection, preparedStatement, index, objectArray);
            } else if (type.equals(Constants.SqlTypes.INT) || type.equals(Constants.SqlTypes.OPTIONAL_INT)) {
                setIntegerArray(connection, preparedStatement, index, objectArray);
            } else if (type.equals(Constants.SqlTypes.BOOLEAN_TYPE) ||
                    type.equals(Constants.SqlTypes.OPTIONAL_BOOLEAN)) {
                setBooleanArray(connection, preparedStatement, index, objectArray);
            } else if (type.equals(Constants.SqlTypes.FLOAT_TYPE) || type.equals(Constants.SqlTypes.OPTIONAL_FLOAT)) {
                setFloatArray(connection, preparedStatement, index, objectArray);
            } else if (type.equals(Constants.SqlTypes.DECIMAL_TYPE) ||
                    type.equals(Constants.SqlTypes.OPTIONAL_DECIMAL)) {
                setDecimalArray(connection, preparedStatement, index, objectArray);
            } else if (type.equals(Constants.SqlTypes.CIVIL_ARRAY_TYPE)) {
                setDateTimeArray(connection, preparedStatement, index, objectArray);
            } else if (type.equals(Constants.SqlTypes.TIME_OF_DAY_ARRAY)) {
                setTimeArray(connection, preparedStatement, index, objectArray);
            } else {
                throw new ApplicationError("Invalid array type[" + type + "] set into the ParameterizedQuery.");
            }
            return Types.ARRAY;
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
            setXml(preparedStatement, index, (BXml) object);
            return Types.SQLXML;
        } else {
            throw new ApplicationError("Unsupported type passed in column index: " + index);
        }
    }

    protected void setXml(PreparedStatement preparedStatement, int index, BXml value) throws SQLException {
        preparedStatement.setObject(index, value.getTextValue(), Types.SQLXML);
    }

    private void setString(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        if (value == null) {
            preparedStatement.setString(index, null);
        } else {
            preparedStatement.setString(index, value.toString());
        }
    }

    private void setNstring(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        if (value == null) {
            preparedStatement.setNString(index, null);
        } else {
            preparedStatement.setNString(index, value.toString());
        }
    }

    private void setBooleanValue(PreparedStatement preparedStatement, String sqlType, int index, Object value)
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
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    private void setNumericAndDecimal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
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
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    private void setBinaryAndBlob(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError, IOException {
        if (value == null) {
            preparedStatement.setBytes(index, null);
        } else if (value instanceof BArray) {
            BArray arrayValue = (BArray) value;
            if (arrayValue.getElementType().getTag() == org.wso2.ballerinalang.compiler.util.TypeTags.BYTE) {
                preparedStatement.setBytes(index, arrayValue.getBytes());
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
        } else if (value instanceof BObject) {
            BObject objectValue = (BObject) value;
            if (objectValue.getType().getName().equalsIgnoreCase(Constants.READ_BYTE_CHANNEL_STRUCT) &&
                    objectValue.getType().getPackage().toString()
                        .equalsIgnoreCase(IOUtils.getIOPackage().toString())) {
                Channel byteChannel = (Channel) objectValue.getNativeData(IOConstants.BYTE_CHANNEL_NAME);
                preparedStatement.setBinaryStream(index, byteChannel.getInputStream());
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    private void setClobAndNclob(Connection connection, PreparedStatement preparedStatement, String sqlType, int index,
            Object value) throws SQLException, ApplicationError {
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
                                .equalsIgnoreCase(IOUtils.getIOPackage().toString())) {
                    CharacterChannel charChannel = (CharacterChannel) objectValue.getNativeData(
                            IOConstants.CHARACTER_CHANNEL_NAME);
                    preparedStatement.setCharacterStream(index, new CharacterChannelReader(charChannel));
                } else {
                    throw Utils.throwInvalidParameterError(value, sqlType);
                }
            }
        }
    }

    private void setRefAndStruct(Connection connection, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        Object[] structData = getStructData(connection, value);
        Object[] dataArray = (Object[]) structData[0];
        String structuredSQLType = (String) structData[1];
        if (dataArray == null) {
            preparedStatement.setNull(index, Types.STRUCT);
        } else {
            Struct struct = connection.createStruct(structuredSQLType, dataArray);
            preparedStatement.setObject(index, struct);
        }
    }

    private void setDateTimeAndTimestamp(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setTimestamp(index, null);
        } else {
            if (value instanceof BString) {
                preparedStatement.setString(index, value.toString());
            }  else if (value instanceof BArray) {
                //this is mapped to time:Utc
                BArray dateTimeStruct = (BArray) value;
                ZonedDateTime zonedDt = TimeValueHandler.createZonedDateTimeFromUtc(dateTimeStruct);
                Timestamp timestamp = new Timestamp(zonedDt.toInstant().toEpochMilli());
                preparedStatement.setTimestamp(index, timestamp, Calendar
                        .getInstance(TimeZone.getTimeZone(Constants.TIMEZONE_UTC.getValue())));
            } else if (value instanceof BMap) {
                //this is mapped to time:Civil
                BMap dateMap = (BMap) value;
                int year = Math.toIntExact(dateMap.getIntValue(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_YEAR)));
                int month = Math.toIntExact(dateMap.getIntValue(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_MONTH)));
                int day = Math.toIntExact(dateMap.getIntValue(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_DAY)));
                int hour = Math.toIntExact(dateMap.getIntValue(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_HOUR)));
                int minute = Math.toIntExact(dateMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_MINUTE)));
                BDecimal second = BDecimal.valueOf(0);
                if (dateMap.containsKey(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_SECOND))) {
                    second = ((BDecimal) dateMap.get(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_SECOND)));
                }
                int zoneHours = 0;
                int zoneMinutes = 0;
                BDecimal zoneSeconds = BDecimal.valueOf(0);
                boolean timeZone = false;
                if (dateMap.containsKey(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET))) {
                    timeZone = true;
                    BMap zoneMap = (BMap) dateMap.get(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET));
                    zoneHours = Math.toIntExact(zoneMap.getIntValue(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)));
                    zoneMinutes = Math.toIntExact(zoneMap.getIntValue(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)));
                    if (zoneMap.containsKey(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND))) {
                        zoneSeconds = ((BDecimal) zoneMap.get(StringUtils.
                                fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)));
                    }
                }
                int intSecond = second.decimalValue().setScale(0, RoundingMode.FLOOR).intValue();
                int intNanoSecond = second.decimalValue().subtract(new BigDecimal(intSecond))
                        .multiply(io.ballerina.stdlib.time.util.Constants.ANALOG_GIGA)
                        .setScale(0, RoundingMode.HALF_UP).intValue();
                LocalDateTime localDateTime = LocalDateTime
                        .of(year, month, day, hour, minute, intSecond, intNanoSecond);
                if (timeZone) {
                    int intZoneSecond = zoneSeconds.decimalValue().setScale(0, RoundingMode.HALF_UP)
                            .intValue();
                    OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime,
                            ZoneOffset.ofHoursMinutesSeconds(zoneHours, zoneMinutes, intZoneSecond));
                    preparedStatement.setObject(index, offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE);
                } else {
                    preparedStatement.setTimestamp(index, Timestamp.valueOf(localDateTime));
                }
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
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
        throw Utils.throwInvalidParameterError(value, Constants.SqlTypes.ARRAY);
    }

    @Override
    protected Object[] getCustomStructData(Connection conn, Object value)
            throws SQLException, ApplicationError {
        Type type = TypeUtils.getType(value);
        String structuredSQLType = type.getName().toUpperCase(Locale.getDefault());
        throw new ApplicationError("unsupported data type of " + structuredSQLType
                + " specified for struct parameter");
    }

    @Override
    protected void setVarchar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setString(preparedStatement, index, value);
    }

    @Override
    protected void setVarcharArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getVarcharValueArrayData(value));
    }

    @Override
    protected void setText(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setString(preparedStatement, index, value);
    }

    @Override
    protected void setChar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setString(preparedStatement, index, value);
    }

    @Override
    protected void setCharArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getCharValueArrayData(value));
    }

    @Override
    protected void setNChar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setNstring(preparedStatement, index, value);
    }

    @Override
    protected void setNVarchar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setNstring(preparedStatement, index, value);
    }

    @Override
    protected void setNVarcharArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getNvarcharValueArrayData(value));
    }

    @Override
    protected void setBit(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        setBooleanValue(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setBitArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getBitValueArrayData(value));
    }

    @Override
    protected void setBoolean(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        setBooleanValue(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setBooleanArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getBooleanValueArrayData(value));
    }

    @Override
    protected void setInteger(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.INTEGER);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setInt(index, ((Number) value).intValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setIntegerArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getIntValueArrayData(value, Constants.SqlArrays.INTEGER,
                "Int Array"));
    }

    @Override
    protected void setBigInt(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.BIGINT);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setLong(index, ((Number) value).longValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setBigIntArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getIntValueArrayData(value, Constants.SqlArrays.BIGINT,
                "Bigint Array"));
    }

    @Override
    protected void setSmallInt(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.SMALLINT);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setShort(index, ((Number) value).shortValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setSmallIntArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getIntValueArrayData(value, Constants.SqlArrays.SMALLINT,
                "Smallint Array"));
    }

    @Override
    protected void setFloat(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.FLOAT);
        } else if (value instanceof Double || value instanceof Long ||
                value instanceof Float || value instanceof Integer) {
            preparedStatement.setFloat(index, ((Number) value).floatValue());
        } else if (value instanceof BDecimal) {
            preparedStatement.setFloat(index, ((BDecimal) value).decimalValue().floatValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setFloatArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getFloatValueArrayData(value));
    }

    @Override
    protected void setReal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.FLOAT);
        } else if (value instanceof Double) {
            preparedStatement.setDouble(index, ((Number) value).doubleValue());
        } else if (value instanceof Long || value instanceof Float || value instanceof Integer) {
            preparedStatement.setFloat(index, ((Number) value).floatValue());
        } else if (value instanceof BDecimal) {
            preparedStatement.setFloat(index, ((BDecimal) value).decimalValue().floatValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setRealArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getRealValueArrayData(value));
    }

    @Override
    protected void setDouble(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setNull(index, Types.DOUBLE);
        } else if (value instanceof Double || value instanceof Long ||
                value instanceof Float || value instanceof Integer) {
            preparedStatement.setDouble(index, ((Number) value).doubleValue());
        } else if (value instanceof BDecimal) {
            preparedStatement.setDouble(index, ((BDecimal) value).decimalValue().doubleValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setDoubleArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getDoubleValueArrayData(value));
    }

    @Override
    protected void setNumeric(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        setNumericAndDecimal(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setNumericArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getNumericValueArrayData(value));
    }

    @Override
    protected void setDecimal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        setNumericAndDecimal(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setDecimalArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getDecimalValueArrayData(value));
    }

    @Override
    protected void setBinary(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError, IOException {
        setBinaryAndBlob(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setBinaryArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError, IOException {
        setPreparedStatement(conn, preparedStatement, index, getBinaryValueArrayData(value));
    }

    @Override
    protected void setVarBinary(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError, IOException {
        setBinaryAndBlob(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setVarBinaryArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError, IOException {
        setPreparedStatement(conn, preparedStatement, index, getVarbinaryValueArrayData(value));
    }

    @Override
    protected void setBlob(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError, IOException {
        setBinaryAndBlob(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setClob(Connection connection, PreparedStatement preparedStatement, String sqlType, int index, 
            Object value)
            throws SQLException, ApplicationError {
        setClobAndNclob(connection, preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setNClob(Connection connection, PreparedStatement preparedStatement, String sqlType, int index, 
            Object value)
            throws SQLException, ApplicationError {
        setClobAndNclob(connection, preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setRow(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setRowId(index, null);
        } else if (value instanceof BArray) {
            BArray arrayValue = (BArray) value;
            if (arrayValue.getElementType().getTag() == org.wso2.ballerinalang.compiler.util.TypeTags.BYTE) {
                RowId rowId = arrayValue::getBytes;
                preparedStatement.setRowId(index, rowId);
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    @Override
    protected void setStruct(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setRefAndStruct(conn, preparedStatement, index, value);
    }

    @Override
    protected void setRef(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setRefAndStruct(conn, preparedStatement, index, value);
    }

    @Override
    protected void setArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError, IOException {
        setPreparedStatement(conn, preparedStatement, index, getArrayData(value));
    }

    private void setPreparedStatement(Connection conn, PreparedStatement preparedStatement, int index,
                                      Object[] arrayData) throws SQLException {
        if (arrayData[0] != null) {
            Array array = conn.createArrayOf((String) arrayData[1], (Object[]) arrayData[0]);
            preparedStatement.setArray(index, array);
        } else {
            preparedStatement.setArray(index, null);
        }
    }

    @Override
    protected void setDateTime(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        setDateTimeAndTimestamp(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setDateTimeArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getDateTimeValueArrayData(value));
    }

    @Override
    protected void setTimestamp(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        setDateTimeAndTimestamp(preparedStatement, sqlType, index, value);
    }

    @Override
    protected void setTimestampArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getTimestampValueArrayData(value));
    }

    @Override
    protected void setDate(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        Date date;
        if (value == null) {
            preparedStatement.setDate(index, null);
        } else {
            if (value instanceof BString) {
                date = Date.valueOf(value.toString());
            } else if (value instanceof BMap) {
                BMap dateMap = (BMap) value;
                int year = Math.toIntExact(dateMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_YEAR)));
                int month = Math.toIntExact(dateMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_MONTH)));
                int day = Math.toIntExact(dateMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.DATE_RECORD_DAY)));
                date = Date.valueOf(year + "-" + month + "-" + day);
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
            preparedStatement.setDate(index, date);
        }
    }

    @Override
    protected void setDateArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getDateValueArrayData(value));
    }

    @Override
    protected void setTime(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError {
        if (value == null) {
            preparedStatement.setTime(index, null);
        } else {
            if (value instanceof BString) {
                preparedStatement.setString(index, value.toString());
            } else if (value instanceof BMap) {
                BMap timeMap = (BMap) value;
                int hour = Math.toIntExact(timeMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_HOUR)));
                int minute = Math.toIntExact(timeMap.getIntValue(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_MINUTE)));
                BDecimal second = BDecimal.valueOf(0);
                if (timeMap.containsKey(StringUtils
                        .fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_SECOND))) {
                    second = ((BDecimal) timeMap.get(StringUtils
                            .fromString(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD_SECOND)));
                }
                int zoneHours = 0;
                int zoneMinutes = 0;
                BDecimal zoneSeconds = BDecimal.valueOf(0);
                boolean timeZone = false;
                if (timeMap.containsKey(StringUtils.
                        fromString(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET))) {
                    timeZone = true;
                    BMap zoneMap = (BMap) timeMap.get(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET));
                    zoneHours = Math.toIntExact(zoneMap.getIntValue(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)));
                    zoneMinutes = Math.toIntExact(zoneMap.getIntValue(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)));
                    if (zoneMap.containsKey(StringUtils.
                            fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND))) {
                        zoneSeconds = ((BDecimal) zoneMap.get(StringUtils.
                                fromString(io.ballerina.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)));
                    }
                }
                int intSecond = second.decimalValue().setScale(0, RoundingMode.FLOOR).intValue();
                int intNanoSecond = second.decimalValue().subtract(new BigDecimal(intSecond))
                        .multiply(io.ballerina.stdlib.time.util.Constants.ANALOG_GIGA)
                        .setScale(0, RoundingMode.HALF_UP).intValue();
                LocalTime localTime = LocalTime.of(hour, minute, intSecond, intNanoSecond);
                if (timeZone) {
                    int intZoneSecond = zoneSeconds.decimalValue().setScale(0, RoundingMode.HALF_UP)
                            .intValue();
                    OffsetTime offsetTime = OffsetTime.of(localTime,
                            ZoneOffset.ofHoursMinutesSeconds(zoneHours, zoneMinutes, intZoneSecond));
                    preparedStatement.setObject(index, offsetTime, Types.TIME_WITH_TIMEZONE);
                } else {
                    preparedStatement.setTime(index, Time.valueOf(localTime));
                }
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
        }
    }

    @Override
    protected void setTimeArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError {
        setPreparedStatement(conn, preparedStatement, index, getTimeValueArrayData(value));
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
            throw Utils.throwInvalidParameterError(value, Constants.SqlTypes.ARRAY);
        }
    }

    @Override
    protected void getRecordStructData(Connection conn, Object[] structData, int i, Object bValue)
            throws SQLException, ApplicationError {
        Object structValue = bValue;
        Object[] internalStructData = getStructData(conn, structValue);
        Object[] dataArray = (Object[]) internalStructData[0];
        String internalStructType = (String) internalStructData[1];
        structValue = conn.createStruct(internalStructType, dataArray);
        structData[i] = structValue;
    }

    @Override
    protected void getArrayStructData(Field field, Object[] structData, String structuredSQLType, int i, Object bValue)
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
