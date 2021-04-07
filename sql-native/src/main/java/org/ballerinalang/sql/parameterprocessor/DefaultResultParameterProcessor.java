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

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.XmlUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BXml;
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.sql.utils.ColumnDefinition;
import org.ballerinalang.sql.utils.ErrorGenerator;
import org.ballerinalang.sql.utils.ModuleUtils;
import org.ballerinalang.sql.utils.Utils;
import org.ballerinalang.stdlib.time.util.TimeValueHandler;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.ballerinalang.stdlib.time.util.Constants.ANALOG_GIGA;

/**
 * This class implements methods required convert SQL types into ballerina types and
 * other methods that process the parameters of the result.
 *
 * @since 0.5.6
 */
public class DefaultResultParameterProcessor extends AbstractResultParameterProcessor {
    private static final Object lock = new Object();
    private static volatile DefaultResultParameterProcessor instance;

    private static final ArrayType stringArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_STRING);
    private static final ArrayType booleanArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_BOOLEAN);
    private static final ArrayType intArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_INT);
    private static final ArrayType floatArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_FLOAT);
    private static final ArrayType decimalArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_DECIMAL);

    public static DefaultResultParameterProcessor getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new DefaultResultParameterProcessor();
                }
            }
        }
        return instance;
    }

    protected BArray createAndPopulateBBRefValueArray(Object firstNonNullElement, Object[] dataArray,
                                                      Type type) throws ApplicationError {
        BArray refValueArray = null;
        int length = dataArray.length;
        if (firstNonNullElement instanceof String) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_STRING);
        } else if (firstNonNullElement instanceof Boolean) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_BOOLEAN);
        } else if (firstNonNullElement instanceof Integer) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_INT);
        } else if (firstNonNullElement instanceof Long) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_INT);
        } else if (firstNonNullElement instanceof Float) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_FLOAT);
        } else if (firstNonNullElement instanceof Double) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_FLOAT);
        } else if (firstNonNullElement instanceof BigDecimal) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_DECIMAL);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i,
                        dataArray[i] != null ? ValueCreator.createDecimalValue((BigDecimal) dataArray[i]) : null);
            }
            return refValueArray;
        } else if (firstNonNullElement == null) {
            refValueArray = createEmptyBBRefValueArray(type);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, firstNonNullElement);
            }
            return refValueArray;
        } else {
            return createAndPopulateCustomBBRefValueArray(firstNonNullElement, dataArray, type);
        }
        for (int i = 0; i < length; i++) {
            refValueArray.add(i, dataArray[i]);
        }
        return refValueArray;
    }

    protected BArray createEmptyBBRefValueArray(Type type) {
        List<Type> memberTypes = new ArrayList<>(2);
        memberTypes.add(type);
        memberTypes.add(PredefinedTypes.TYPE_NULL);
        UnionType unionType = TypeCreator.createUnionType(memberTypes);
        return ValueCreator.createArrayValue(TypeCreator.createArrayType(unionType));
    }

    @Override
    protected BArray createAndPopulateCustomBBRefValueArray(Object firstNonNullElement, Object[] dataArray,
                                                            Type type) {
        return null;
    }

    protected BArray createAndPopulatePrimitiveValueArray(Object firstNonNullElement, Object[] dataArray)
            throws ApplicationError {
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
            return createAndPopulateCustomValueArray(firstNonNullElement, dataArray);
        }
    }

    @Override
    protected BArray createAndPopulateCustomValueArray(Object firstNonNullElement, Object[] dataArray) {
        return null;
    }

    protected BMap<BString, Object> createUserDefinedType(Struct structValue, StructureType structType)
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
                            createUserDefinedTypeSubtype(internalField, structType);
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

    @Override
    protected void createUserDefinedTypeSubtype(Field internalField, StructureType structType)
            throws ApplicationError {
        throw new ApplicationError("Error while retrieving data for unsupported type " +
                internalField.getFieldType().getName() + " to create "
                + structType.getName() + " record.");
    }

    protected static boolean containsNullObject(Object[] objects) {
        for (Object object : objects) {
            if (object == null) {
                return true;
            }
        }
        return false;
    }

    protected static Object firstNonNullObject(Object[] objects) {
        for (Object object : objects) {
            if (object != null) {
                return object;
            }
        }
        return null;
    }

    @Override
    public BArray convertArray(Array array, int sqlType, Type type) throws SQLException, ApplicationError {
        if (array != null) {
            Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Array");
            Object[] dataArray = (Object[]) array.getArray();
            if (dataArray == null || dataArray.length == 0) {
                return null;
            }

            Object firstNonNullElement = firstNonNullObject(dataArray);
            boolean containsNull = containsNullObject(dataArray);

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

    @Override
    public BString convertChar(String value, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL String");
        return fromString(value);
    }

    @Override
    public Object convertChar(
            String value, int sqlType, Type type, String sqlTypeName) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, sqlTypeName);
        return fromString(value);
    }

    @Override
    public Object convertByteArray(byte[] value, int sqlType, Type type, String sqlTypeName) throws ApplicationError {
        if (value != null) {
            return ValueCreator.createArrayValue(value);
        } else {
            return null;
        }
    }

    @Override
    public Object convertBinary(Object value, int sqlType, Type ballerinaType) throws ApplicationError {
        if (ballerinaType.getTag() == TypeTags.STRING_TAG) {
            return convertChar((String) value, sqlType, ballerinaType);
        } else {
            return convertByteArray(((String) value).getBytes(Charset.defaultCharset()), sqlType, ballerinaType,
                    JDBCType.valueOf(sqlType).getName()
            );
        }
    }

    @Override
    public Object convertInteger(long value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL long or integer");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return value;
        }
    }

    @Override
    public Object convertDouble(double value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL double or float");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return value;
        }
    }

    @Override
    public Object convertDecimal(BigDecimal value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL decimal or real");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return ValueCreator.createDecimalValue(value);
        }
    }

    @Override
    public Object convertBlob(Blob value, int sqlType, Type type) throws ApplicationError, SQLException {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Blob");
        if (value != null) {
            return ValueCreator.createArrayValue(value.getBytes(1L, (int) value.length()));
        } else {
            return null;
        }
    }

    @Override
    public Object convertDate(java.util.Date date, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (date != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(date.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equals(org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD)) {
                        if (date instanceof Date) {
                            LocalDate dateObj = ((Date) date).toLocalDate();
                            BMap<BString, Object> dateMap = ValueCreator.createRecordValue(
                                    org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                                    org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD);
                            dateMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_YEAR),
                                    dateObj.getYear());
                            dateMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_MONTH),
                                    dateObj.getMonthValue());
                            dateMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_DAY),
                                    dateObj.getDayOfMonth());
                            return dateMap;
                        } else {
                            return fromString(date.toString());
                        }
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Date Data type.");
                    }
                case TypeTags.INT_TAG:
                    return date.getTime();
            }
        }
        return null;
    }

    @Override
    public Object convertTime(java.util.Date time, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (time != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(time.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equals(org.ballerinalang.stdlib.time.util.Constants.TIME_OF_DAY_RECORD)) {
                        if (time instanceof Time) {
                            LocalTime timeObj = ((Time) time).toLocalTime();
                            BMap<BString, Object> timeMap = ValueCreator.createRecordValue(
                                    org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                                    org.ballerinalang.stdlib.time.util.Constants.TIME_OF_DAY_RECORD);
                            timeMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                    .TIME_OF_DAY_RECORD_HOUR), timeObj.getHour());
                            timeMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                    .TIME_OF_DAY_RECORD_MINUTE) , timeObj.getMinute());
                            BigDecimal second = new BigDecimal(timeObj.getSecond());
                            second = second.add(new BigDecimal(timeObj.getNano())
                                    .divide(ANALOG_GIGA, MathContext.DECIMAL128));
                            timeMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                    .TIME_OF_DAY_RECORD_SECOND), ValueCreator.createDecimalValue(second));
                            return timeMap;
                        } else {
                            return fromString(time.toString());
                        }
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Time Data type.");
                    }
                case TypeTags.INT_TAG:
                    return time.getTime();
            }
        }
        return null;
    }

    @Override
    public Object convertTimeWithTimezone(java.time.OffsetTime offsetTime, int sqlType, Type type)
            throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (offsetTime != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(offsetTime.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equals(org.ballerinalang.stdlib.time.util.Constants.TIME_OF_DAY_RECORD)) {
                        BMap<BString, Object> timeMap = ValueCreator.createRecordValue(
                                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                                org.ballerinalang.stdlib.time.util.Constants.TIME_OF_DAY_RECORD);
                        timeMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_HOUR), offsetTime.getHour());
                        timeMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_MINUTE), offsetTime.getMinute());
                        BigDecimal second = new BigDecimal(offsetTime.getSecond());
                        second = second.add(new BigDecimal(offsetTime.getNano()).divide(ANALOG_GIGA,
                                MathContext.DECIMAL128));
                        timeMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_SECOND), ValueCreator.createDecimalValue(second));
                        Map<String, Integer> zoneInfo = TimeValueHandler
                                .zoneOffsetMapFromString(offsetTime.getOffset().toString());
                        BMap<BString, Object> zoneMap = ValueCreator.createRecordValue(
                                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                                org.ballerinalang.stdlib.time.util.Constants.READABLE_ZONE_OFFSET_RECORD);
                        if (zoneInfo
                            .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR) != null) {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR),
                                    zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)
                                            .longValue());
                        } else {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR), 0);
                        }
                        if (zoneInfo
                            .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE) != null) {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE),
                                    zoneInfo
                                        .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)
                                        .longValue());
                        } else {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE), 0);
                        }
                        if (zoneInfo
                            .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND) != null) {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND),
                                    zoneInfo
                                    .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)
                                    .longValue());
                        }
                        zoneMap.freezeDirect();
                        timeMap.put(StringUtils.fromString(
                                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET), zoneMap);
                        timeMap.put(StringUtils.fromString(
                                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD_TIME_ABBREV),
                                StringUtils.fromString(offsetTime.getOffset().toString()));
                        return timeMap;
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Time with timezone Data type.");
                    }
                case TypeTags.INT_TAG:
                    return Time.valueOf(offsetTime.toLocalTime()).getTime();
            }
        }
        return null;
    }

    @Override
    public Object convertTimeStamp(java.util.Date timestamp, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (timestamp != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(timestamp.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equalsIgnoreCase(org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD)
                            && timestamp instanceof Timestamp) {
                        LocalDateTime dateTimeObj = ((Timestamp) timestamp).toLocalDateTime();
                        BMap<BString, Object> civilMap = ValueCreator.createRecordValue(
                                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD);
                        civilMap.put(StringUtils.fromString(
                                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_YEAR), dateTimeObj.getYear());
                        civilMap.put(StringUtils.fromString(
                                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_MONTH),
                                dateTimeObj.getMonthValue());
                        civilMap.put(StringUtils.fromString(
                                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_DAY),
                                dateTimeObj.getDayOfMonth());
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_HOUR), dateTimeObj.getHour());
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_MINUTE), dateTimeObj.getMinute());
                        BigDecimal second = new BigDecimal(dateTimeObj.getSecond());
                        second = second.add(new BigDecimal(dateTimeObj.getNano())
                                .divide(ANALOG_GIGA, MathContext.DECIMAL128));
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_SECOND), ValueCreator.createDecimalValue(second));
                        return civilMap;
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Timestamp Data type.");
                    }
                case TypeTags.INT_TAG:
                    return timestamp.getTime();
                case TypeTags.INTERSECTION_TAG:
                    return Utils.createTimeStruct(timestamp.getTime());
            }
        }
        return null;
    }

    @Override
    public Object convertTimestampWithTimezone(java.time.OffsetDateTime offsetDateTime, int sqlType, Type type)
            throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (offsetDateTime != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(offsetDateTime.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equalsIgnoreCase(org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD)) {
                        BMap<BString, Object> civilMap = ValueCreator.createRecordValue(
                                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD);
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                        .DATE_RECORD_YEAR), offsetDateTime.getYear());
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                        .DATE_RECORD_MONTH), offsetDateTime.getMonthValue());
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                        .DATE_RECORD_DAY), offsetDateTime.getDayOfMonth());
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_HOUR), offsetDateTime.getHour());
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_MINUTE), offsetDateTime.getMinute());
                        BigDecimal second = new BigDecimal(offsetDateTime.getSecond());
                        second = second.add(new BigDecimal(offsetDateTime.getNano()).divide(ANALOG_GIGA,
                                MathContext.DECIMAL128));
                        civilMap.put(StringUtils.fromString(org.ballerinalang.stdlib.time.util.Constants
                                .TIME_OF_DAY_RECORD_SECOND), ValueCreator.createDecimalValue(second));
                        Map<String, Integer> zoneInfo = TimeValueHandler
                                .zoneOffsetMapFromString(offsetDateTime.getOffset().toString());
                        BMap<BString, Object> zoneMap = ValueCreator.createRecordValue(
                                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                                org.ballerinalang.stdlib.time.util.Constants.READABLE_ZONE_OFFSET_RECORD);
                        if (zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)
                                != null) {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR),
                                    zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)
                                            .longValue());
                        } else {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR), 0);
                        }
                        if (zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)
                                != null) {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE),
                                    zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)
                                            .longValue());
                        } else {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE), 0);
                        }
                        if (zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)
                                != null) {
                            zoneMap.put(StringUtils.fromString(
                                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND),
                                    zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)
                                            .longValue());
                        }
                        zoneMap.freezeDirect();
                        civilMap.put(StringUtils.fromString(
                                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET), zoneMap);
                        civilMap.put(StringUtils.fromString(
                                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD_TIME_ABBREV),
                                StringUtils.fromString(offsetDateTime.getOffset().toString()));
                        return civilMap;
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Timestamp with timezone Data type.");
                    }
                case TypeTags.INT_TAG:
                    return Timestamp.valueOf(offsetDateTime.toLocalDateTime()).getTime();
                case TypeTags.INTERSECTION_TAG:
                    return Utils.createTimeStruct(Timestamp.valueOf(offsetDateTime.toLocalDateTime()).getTime());
            }
        }
        return null;
    }

    @Override
    public Object convertBoolean(boolean value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Boolean");
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

    @Override
    public Object convertStruct(Struct value, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Struct");
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

    @Override
    public Object convertXml(SQLXML value, int sqlType, Type type) throws ApplicationError, SQLException {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL XML");
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

    private void populateCharacterTypes(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getString(paramIndex));
    }

    private void populateFloatAndReal(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getFloat(paramIndex));
    }

    private void populateNumericAndDecimal(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getBigDecimal(paramIndex));
    }

    private void populateBitAndBoolean(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getBoolean(paramIndex));
    }

    private void populateRefAndStruct(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getObject(paramIndex));
    }

    @Override
    public void populateChar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override
    public void populateVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override
    public void populateLongVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override
    public void populateNChar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override
    public void populateNVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override
    public void populateLongNVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override
    public void populateBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override

    public void populateVarBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override
    public void populateLongVarBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateCharacterTypes(statement, parameter, paramIndex);
    }

    @Override
    public void populateBlob(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getBlob(paramIndex));
    }

    @Override
    public void populateClob(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getClob(paramIndex));
    }

    @Override
    public void populateNClob(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getNClob(paramIndex));
    }

    @Override
    public void populateDate(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getDate(paramIndex));
    }

    @Override
    public void populateTime(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getTime(paramIndex));
    }

    @Override
    public void populateTimeWithTimeZone(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        OffsetTime offsetTime = statement.getObject(paramIndex, OffsetTime.class);
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA, offsetTime);
    }

    @Override
    public void populateTimestamp(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getTimestamp(paramIndex));
    }

    @Override
    public void populateTimestampWithTimeZone(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        OffsetDateTime offsetDateTime = statement.getObject(paramIndex, OffsetDateTime.class);
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA, offsetDateTime);
    }

    @Override
    public void populateArray(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getArray(paramIndex));
    }

    @Override
    public void populateRowID(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getRowId(paramIndex));
    }

    @Override
    public void populateTinyInt(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA, statement.getInt(paramIndex));
    }

    @Override
    public void populateSmallInt(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                Integer.valueOf(statement.getShort(paramIndex)));
    }

    @Override
    public void populateInteger(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                Long.valueOf(statement.getInt(paramIndex)));
    }

    @Override
    public void populateBigInt(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA, statement.getLong(paramIndex));
    }

    @Override
    public void populateReal(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        populateFloatAndReal(statement, parameter, paramIndex);
    }

    @Override
    public void populateFloat(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException {
        populateFloatAndReal(statement, parameter, paramIndex);
    }

    @Override
    public void populateDouble(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getDouble(paramIndex));
    }

    @Override
    public void populateNumeric(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        populateNumericAndDecimal(statement, parameter, paramIndex);

    }

    @Override
    public void populateDecimal(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        populateNumericAndDecimal(statement, parameter, paramIndex);
    }

    @Override
    public void populateBit(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        populateBitAndBoolean(statement, parameter, paramIndex);

    }

    @Override
    public void populateBoolean(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        populateBitAndBoolean(statement, parameter, paramIndex);
    }

    @Override
    public void populateRef(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        populateRefAndStruct(statement, parameter, paramIndex);
    }

    @Override
    public void populateStruct(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        populateRefAndStruct(statement, parameter, paramIndex);
    }

    @Override
    public void populateXML(CallableStatement statement, BObject parameter, int paramIndex) throws SQLException {
        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                statement.getSQLXML(paramIndex));
    }

    @Override
    public void populateCustomOutParameters(CallableStatement statement, BObject parameter, int paramIndex, int sqlType)
            throws ApplicationError {
        throw new ApplicationError("Unsupported SQL type '" + sqlType + "' when reading Procedure call " +
                "Out parameter of index '" + paramIndex + "'.");
    }

    @Override
    public Object getCustomOutParameters(BObject value, int sqlType, Type ballerinaType) {
        return ErrorGenerator.getSQLApplicationError("Unsupported SQL type " + sqlType);
    }

    protected BObject getIteratorObject() {
        return null;
    }

    public BObject createRecordIterator(ResultSet resultSet,
                                        Statement statement,
                                        Connection connection, List<ColumnDefinition> columnDefinitions,
                                        StructureType streamConstraint) {
        BObject iteratorObject = this.getIteratorObject();
        BObject resultIterator = ValueCreator.createObjectValue(ModuleUtils.getModule(),
                Constants.RESULT_ITERATOR_OBJECT, new Object[]{null, iteratorObject});
        resultIterator.addNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD, resultSet);
        resultIterator.addNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD, statement);
        resultIterator.addNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD, connection);
        resultIterator.addNativeData(Constants.COLUMN_DEFINITIONS_DATA_FIELD, columnDefinitions);
        resultIterator.addNativeData(Constants.RECORD_TYPE_DATA_FIELD, streamConstraint);
        return resultIterator;
    }

    public Object getCustomResult(ResultSet resultSet, int columnIndex, ColumnDefinition columnDefinition)
            throws ApplicationError {
        throw new ApplicationError("Unsupported SQL type " + columnDefinition.getSqlName());
    }

    public BObject getCustomProcedureCallObject() {
        return null;
    }
}
