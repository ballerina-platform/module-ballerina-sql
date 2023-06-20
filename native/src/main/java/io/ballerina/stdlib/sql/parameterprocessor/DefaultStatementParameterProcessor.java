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
import io.ballerina.runtime.api.types.ObjectType;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BValue;
import io.ballerina.runtime.api.values.BXml;
import io.ballerina.stdlib.io.channels.base.Channel;
import io.ballerina.stdlib.io.channels.base.CharacterChannel;
import io.ballerina.stdlib.io.readers.CharacterChannelReader;
import io.ballerina.stdlib.io.utils.IOConstants;
import io.ballerina.stdlib.io.utils.IOUtils;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ConversionError;
import io.ballerina.stdlib.sql.exception.DataError;
import io.ballerina.stdlib.sql.exception.TypeMismatchError;
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
import java.sql.SQLFeatureNotSupportedException;
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
import java.time.format.DateTimeParseException;
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

    private Object[] getArrayData(Object value) throws DataError, SQLException {
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

    protected Object[] getIntValueArrayData(Object value, String type, String dataType) throws DataError {
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

    protected Object[] getDecimalValueArrayData(Object value) throws DataError {
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

    protected Object[] getRealValueArrayData(Object value) throws DataError {
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

    protected Object[] getNumericValueArrayData(Object value) throws DataError {
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

    protected Object[] getDoubleValueArrayData(Object value) throws DataError {
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

    protected Object[] getFloatValueArrayData(Object value) throws DataError {
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

    protected Object[] getCharValueArrayData(Object value) throws DataError {
        return getStringValueArrayData(value, Constants.SqlArrays.CHAR);
    }

    protected Object[] getVarcharValueArrayData(Object value) throws DataError {
        return getStringValueArrayData(value, Constants.SqlArrays.VARCHAR);
    }

    protected Object[] getNvarcharValueArrayData(Object value) throws DataError {
        return getStringValueArrayData(value, Constants.SqlArrays.NVARCHAR);
    }

    protected Object[] getDateTimeValueArrayData(Object value) throws DataError {
        return getDateTimeAndTimestampValueArrayData(value);
    }

    protected Object[] getTimestampValueArrayData(Object value) throws DataError {
        return getDateTimeAndTimestampValueArrayData(value);
    }

    protected Object[] getBooleanValueArrayData(Object value) throws DataError {
        return getBitAndBooleanValueArrayData(value, Constants.SqlArrays.BOOLEAN);
    }

    protected Object[] getDateValueArrayData(Object value) throws DataError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Date[arrayLength];
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            try {
                if (innerValue == null) {
                    arrayData[i] = null;
                } else if (innerValue instanceof BString) {
                    arrayData[i] = Date.valueOf(innerValue.toString());
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
            } catch (ArithmeticException | IllegalArgumentException ex) {
                throw new ConversionError("Unsupported value: " + innerValue + " for Date Array");
            }
        }
        return new Object[]{arrayData, Constants.SqlArrays.DATE};
    }

    protected Object[] getTimeValueArrayData(Object value) throws DataError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        Object[] arrayData = new Object[arrayLength];
        boolean containsTimeZone = false;
        BArray array = ((BArray) value);
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            try {
                if (innerValue == null) {
                    arrayData[i] = null;
                } else if (innerValue instanceof BString) {
                    arrayData[i] = Time.valueOf(innerValue.toString());
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
            } catch (ArithmeticException | IllegalArgumentException ex) {
                throw new ConversionError("Unsupported value " + innerValue + " for Time Array");
            }
        }
        if (containsTimeZone) {
            return new Object[]{arrayData, Constants.SqlArrays.TIME_WITH_TIMEZONE};
        } else {
            return new Object[]{arrayData, Constants.SqlArrays.TIME};
        }
    }

    protected Object[] getBinaryValueArrayData(Object value) throws DataError {
        return getBinaryAndBlobValueArrayData(value, Constants.SqlArrays.BINARY);
    }

    protected Object[] getVarbinaryValueArrayData(Object value) throws DataError {
        return getBinaryAndBlobValueArrayData(value, Constants.SqlArrays.VARBINARY);
    }

    protected Object[] getBitValueArrayData(Object value) throws DataError {
        return getBitAndBooleanValueArrayData(value, Constants.SqlArrays.BIT);
    }

    private Object[] getBitAndBooleanValueArrayData(Object value, String type) throws DataError {
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
                    throw new DataError("Only 1 or 0 can be passed for " + type
                            + " SQL Type, but found :" + lVal);
                }
            } else if (innerValue instanceof Boolean) {
                arrayData[i] = innerValue;
            } else {
                throw Utils.throwInvalidParameterError(innerValue, type + " Array");
            }
        }
        return new Object[]{arrayData, type};
    }

    private Object[] getBinaryAndBlobValueArrayData(Object value, String type) throws DataError {
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
                ObjectType objectValueType = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(objectValue));
                if (objectValueType.getName().equalsIgnoreCase(Constants.READ_BYTE_CHANNEL_STRUCT) &&
                        objectValueType.getPackage().toString()
                                .equalsIgnoreCase(IOUtils.getIOPackage().toString())) {
                    try {
                        Channel byteChannel = (Channel) objectValue.getNativeData(IOConstants.BYTE_CHANNEL_NAME);
                        arrayData[i] = toByteArray(byteChannel.getInputStream());
                    } catch (IOException e) {
                        throw new DataError("Error processing byte channel ", e);
                    }
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

    private Object[] getDateTimeAndTimestampValueArrayData(Object value) throws DataError {
        int arrayLength = ((BArray) value).size();
        Object innerValue;
        boolean containsTimeZone = false;
        BArray array = ((BArray) value);
        Object[] arrayData = new Object[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            innerValue = array.get(i);
            try {
                if (innerValue == null) {
                    arrayData[i] = null;
                } else if (innerValue instanceof BString) {
                    java.time.format.DateTimeFormatter formatter =
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    arrayData[i] = LocalDateTime.parse(innerValue.toString(), formatter);
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
                        arrayData[i] = offsetDateTime;
                    } else {
                        arrayData[i] = Timestamp.valueOf(localDateTime);
                    }
                } else {
                    throw Utils.throwInvalidParameterError(value, "TIMESTAMP ARRAY");
                }
            } catch (DateTimeParseException | ArithmeticException e) {
                throw new ConversionError("Unsupported value: " + innerValue + " for DateTime Array");
            }
        }
        if (containsTimeZone) {
            return new Object[]{arrayData, Constants.SqlArrays.TIMESTAMP_WITH_TIMEZONE};
        } else {
            return new Object[]{arrayData, Constants.SqlArrays.TIMESTAMP};
        }
    }

    private Object[] getStringValueArrayData(Object value, String type) throws DataError {
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

    private Object[] getStructData(Connection conn, Object value) throws SQLException, DataError {
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
            int typeTag = TypeUtils.getReferredType(field.getFieldType()).getTag();
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
                    getCustomStructData(value);
            }
        }
        return new Object[]{structData, structuredSQLType};
    }

    protected void setXml(Connection connection, PreparedStatement preparedStatement, int index, BXml value)
            throws SQLException, DataError {
        preparedStatement.setObject(index, value.getTextValue(), Types.SQLXML);
    }

    @Override
    protected int setCustomBOpenRecord(Connection connection, PreparedStatement preparedStatement, int index,
                                       Object value, boolean returnType) throws DataError, SQLException {
        throw new DataError("Unsupported type passed in column index: " + index);
    }

    private void setString(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        if (value == null) {
            preparedStatement.setString(index, null);
        } else {
            preparedStatement.setString(index, value.toString());
        }
    }

    private void setNString(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        if (value == null) {
            preparedStatement.setNString(index, null);
        } else {
            preparedStatement.setNString(index, value.toString());
        }
    }

    private void setBooleanValue(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        if (value == null) {
            preparedStatement.setNull(index, Types.BOOLEAN);
        } else if (value instanceof BString) {
            preparedStatement.setBoolean(index, Boolean.parseBoolean(value.toString()));
        } else if (value instanceof Integer || value instanceof Long) {
            long lVal = ((Number) value).longValue();
            if (lVal == 1 || lVal == 0) {
                preparedStatement.setBoolean(index, lVal == 1);
            } else {
                throw new DataError("Only 1 or 0 can be passed for " + sqlType
                        + " SQL Type, but found :" + lVal);
            }
        } else if (value instanceof Boolean) {
            preparedStatement.setBoolean(index, (Boolean) value);
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    private void setNumericAndDecimal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
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
            throws SQLException, DataError {
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
            ObjectType objectValueType = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(objectValue));
            if (objectValueType.getName().equalsIgnoreCase(Constants.READ_BYTE_CHANNEL_STRUCT) &&
                    objectValueType.getPackage().toString()
                            .equalsIgnoreCase(IOUtils.getIOPackage().toString())) {
                try {
                    Channel byteChannel = (Channel) objectValue.getNativeData(IOConstants.BYTE_CHANNEL_NAME);
                    preparedStatement.setBinaryStream(index, byteChannel.getInputStream());
                } catch (IOException e) {
                    throw new DataError("Error when processing binary stream.", e);
                }
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    private void setClobAndNclob(Connection connection, PreparedStatement preparedStatement, String sqlType, int index,
                                 Object value) throws SQLException, DataError {
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
                ObjectType objectValueType = (ObjectType) TypeUtils.getReferredType(((BValue) objectValue).getType());
                if (objectValueType.getName().equalsIgnoreCase(Constants.READ_CHAR_CHANNEL_STRUCT) &&
                        objectValueType.getPackage().toString()
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
            throws SQLException, DataError {
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
            throws SQLException, DataError {
        BMap zoneMap = null;
        if (value == null) {
            preparedStatement.setTimestamp(index, null);
        } else {
            if (value instanceof BString) {
                preparedStatement.setString(index, value.toString());
            } else if (value instanceof BArray) {
                //this is mapped to time:Utc
                BArray dateTimeStruct = (BArray) value;
                ZonedDateTime zonedDt = TimeValueHandler.createZonedDateTimeFromUtc(dateTimeStruct);
                Timestamp timestamp = new Timestamp(zonedDt.toInstant().toEpochMilli());
                preparedStatement.setTimestamp(index, timestamp, Calendar
                        .getInstance(TimeZone.getTimeZone(Constants.TIMEZONE_UTC.getValue())));
            } else if (value instanceof BMap) {
                //this is mapped to time:Civil
                try {
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
                        zoneMap = (BMap) dateMap.get(StringUtils.
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
                } catch (Throwable ex) {
                    if (ex instanceof SQLFeatureNotSupportedException) {
                        if (zoneMap != null) {
                            throw new TypeMismatchError("Unsupported type: offset of Civil is not supported by " +
                                    "this database. " + ex.getMessage());
                        } else {
                            throw new TypeMismatchError("Unsupported type: " + ex.getMessage());
                        }
                    }
                    throw new ConversionError("Unsupported value: " + value + " for " + sqlType);
                }
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
        }
    }

    public int getCustomOutParameterType(BObject typedValue) throws DataError, SQLException {
        String sqlType = TypeUtils.getType(typedValue).getName();
        throw new DataError("Unsupported OutParameter type: " + sqlType);
    }

    protected int getCustomSQLType(BObject typedValue) throws DataError, SQLException {
        String sqlType = ((BValue) typedValue).getType().getName();
        throw new DataError("Unsupported SQL type: " + sqlType);
    }

    protected void setCustomSqlTypedParam(Connection connection, PreparedStatement preparedStatement,
                                          int index, BObject typedValue)
            throws SQLException, DataError {
        String sqlType = ((BValue) typedValue).getType().getName();
        throw new DataError("Unsupported SQL type: " + sqlType);
    }

    protected Object[] getCustomArrayData(Object value) throws DataError, SQLException {
        throw Utils.throwInvalidParameterError(value, Constants.SqlTypes.ARRAY);
    }

    protected Object[] getCustomStructData(Object value) throws DataError, SQLException {
        Type type = TypeUtils.getType(value);
        String structuredSQLType = type.getName().toUpperCase(Locale.getDefault());
        throw new DataError("unsupported data type of " + structuredSQLType
                + " specified for struct parameter");
    }

    protected void setVarchar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setString(preparedStatement, index, value);
    }

    protected void setVarcharArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getVarcharValueArrayData(value));
    }

    protected void setText(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setString(preparedStatement, index, value);
    }

    protected void setChar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setString(preparedStatement, index, value);
    }

    protected void setCharArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getCharValueArrayData(value));
    }

    protected void setNChar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setNString(preparedStatement, index, value);
    }

    protected void setNVarchar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException {
        setNString(preparedStatement, index, value);
    }

    protected void setNVarcharArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getNvarcharValueArrayData(value));
    }

    protected void setBit(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setBooleanValue(preparedStatement, sqlType, index, value);
    }

    protected void setBitArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getBitValueArrayData(value));
    }

    protected void setBoolean(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setBooleanValue(preparedStatement, sqlType, index, value);
    }

    protected void setBooleanArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getBooleanValueArrayData(value));
    }

    protected void setInteger(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        if (value == null) {
            preparedStatement.setNull(index, Types.INTEGER);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setInt(index, ((Number) value).intValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    protected void setIntegerArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getIntValueArrayData(value, Constants.SqlArrays.INTEGER,
                "Int Array"));
    }

    protected void setBigInt(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        if (value == null) {
            preparedStatement.setNull(index, Types.BIGINT);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setLong(index, ((Number) value).longValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    protected void setBigIntArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getIntValueArrayData(value, Constants.SqlArrays.BIGINT,
                "Bigint Array"));
    }

    protected void setSmallInt(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        if (value == null) {
            preparedStatement.setNull(index, Types.SMALLINT);
        } else if (value instanceof Integer || value instanceof Long) {
            preparedStatement.setShort(index, ((Number) value).shortValue());
        } else {
            throw Utils.throwInvalidParameterError(value, sqlType);
        }
    }

    protected void setSmallIntArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getIntValueArrayData(value, Constants.SqlArrays.SMALLINT,
                "Smallint Array"));
    }

    protected void setFloat(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
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

    protected void setFloatArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getFloatValueArrayData(value));
    }

    protected void setReal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
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

    protected void setRealArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getRealValueArrayData(value));
    }

    protected void setDouble(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
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

    protected void setDoubleArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getDoubleValueArrayData(value));
    }

    protected void setNumeric(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setNumericAndDecimal(preparedStatement, sqlType, index, value);
    }

    protected void setNumericArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getNumericValueArrayData(value));
    }

    protected void setDecimal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setNumericAndDecimal(preparedStatement, sqlType, index, value);
    }

    protected void setDecimalArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getDecimalValueArrayData(value));
    }

    protected void setBinary(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setBinaryAndBlob(preparedStatement, sqlType, index, value);
    }

    protected void setBinaryArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getBinaryValueArrayData(value));
    }

    protected void setVarBinary(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setBinaryAndBlob(preparedStatement, sqlType, index, value);
    }

    protected void setVarBinaryArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getVarbinaryValueArrayData(value));
    }

    protected void setBlob(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setBinaryAndBlob(preparedStatement, sqlType, index, value);
    }

    protected void setClob(Connection connection, PreparedStatement preparedStatement, String sqlType, int index,
                           Object value)
            throws SQLException, DataError {
        setClobAndNclob(connection, preparedStatement, sqlType, index, value);
    }

    protected void setNClob(Connection connection, PreparedStatement preparedStatement, String sqlType, int index,
                            Object value)
            throws SQLException, DataError {
        setClobAndNclob(connection, preparedStatement, sqlType, index, value);
    }

    protected void setRow(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
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

    protected void setStruct(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setRefAndStruct(conn, preparedStatement, index, value);
    }

    protected void setRef(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setRefAndStruct(conn, preparedStatement, index, value);
    }

    protected void setArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
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

    protected void setDateTime(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setDateTimeAndTimestamp(preparedStatement, sqlType, index, value);
    }

    protected void setDateTimeArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getDateTimeValueArrayData(value));
    }

    protected void setTimestamp(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        setDateTimeAndTimestamp(preparedStatement, sqlType, index, value);
    }

    protected void setTimestampArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getTimestampValueArrayData(value));
    }

    protected void setDate(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        Date date;
        if (value == null) {
            preparedStatement.setDate(index, null);
        } else {
            try {
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
            } catch (IllegalArgumentException | ArithmeticException e) {
                throw new ConversionError("Unsupported value: " + value + " for Date Value");
            }
            preparedStatement.setDate(index, date);
        }
    }

    protected void setDateArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getDateValueArrayData(value));
    }

    protected void setTime(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, DataError {
        BMap zoneMap = null;
        if (value == null) {
            preparedStatement.setTime(index, null);
        } else {
            if (value instanceof BString) {
                preparedStatement.setString(index, value.toString());
            } else if (value instanceof BMap) {
                try {
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
                        zoneMap = (BMap) timeMap.get(StringUtils.
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
                } catch (Throwable ex) {
                    if (ex instanceof SQLFeatureNotSupportedException) {
                        if (zoneMap != null) {
                            throw new TypeMismatchError("Unsupported type: offset of Civil is not supported by " +
                                    "this database. " + ex.getMessage());
                        } else {
                            throw new TypeMismatchError("Unsupported type: " + ex.getMessage());
                        }
                    }
                    throw new ConversionError("Unsupported value: " + value + " for Time Value");
                }
            } else {
                throw Utils.throwInvalidParameterError(value, sqlType);
            }
        }
    }

    protected void setTimeArray(Connection conn, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, DataError {
        setPreparedStatement(conn, preparedStatement, index, getTimeValueArrayData(value));
    }

    protected Object[] getIntArrayData(Object value) {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new Long[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BArray) value).getInt(i);
        }
        return new Object[]{arrayData, "BIGINT"};
    }

    protected Object[] getFloatArrayData(Object value) {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new Double[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BArray) value).getFloat(i);
        }
        return new Object[]{arrayData, "DOUBLE"};
    }

    protected Object[] getDecimalArrayData(Object value) {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new BigDecimal[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BDecimal) ((BArray) value).getRefValue(i)).value();
        }
        return new Object[]{arrayData, "DECIMAL"};
    }

    protected Object[] getStringArrayData(Object value) {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new String[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BArray) value).getBString(i).getValue();
        }
        return new Object[]{arrayData, "VARCHAR"};
    }

    protected Object[] getBooleanArrayData(Object value) {
        int arrayLength = ((BArray) value).size();
        Object[] arrayData = new Boolean[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            arrayData[i] = ((BArray) value).getBoolean(i);
        }
        return new Object[]{arrayData, "BOOLEAN"};
    }

    protected Object[] getNestedArrayData(Object value) throws DataError {
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

    protected void getRecordStructData(Connection conn, Object[] structData, int i, Object bValue)
            throws SQLException, DataError {
        Object structValue = bValue;
        Object[] internalStructData = getStructData(conn, structValue);
        Object[] dataArray = (Object[]) internalStructData[0];
        String internalStructType = (String) internalStructData[1];
        structValue = conn.createStruct(internalStructType, dataArray);
        structData[i] = structValue;
    }

    protected void getArrayStructData(Field field, Object[] structData, String structuredSQLType, int i, Object bValue)
            throws DataError {
        Type elementType = ((ArrayType) field
                .getFieldType()).getElementType();
        if (elementType.getTag() == TypeTags.BYTE_TAG) {
            structData[i] = ((BArray) bValue).getBytes();
        } else {
            throw new DataError("unsupported data type of " + structuredSQLType
                    + " specified for struct parameter");
        }
    }
}
