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
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BValue;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.stdlib.time.util.TimeValueHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.ballerinalang.sql.Constants.AFFECTED_ROW_COUNT_FIELD;
import static org.ballerinalang.sql.Constants.EXECUTION_RESULT_FIELD;
import static org.ballerinalang.sql.Constants.EXECUTION_RESULT_RECORD;
import static org.ballerinalang.stdlib.time.util.Constants.ANALOG_GIGA;

/**
 * This class has the utility methods to process and convert the SQL types into ballerina types,
 * and other shared utility methods.
 *
 * @since 1.2.0
 */
public class Utils {

    public static void closeResources(
            TransactionResourceManager trxResourceManager, ResultSet resultSet, Statement statement,
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

    public static String getSqlQuery(BObject paramString) {
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


    public static ApplicationError throwInvalidParameterError(Object value, String sqlType) {
        String valueName;
        if (value instanceof BValue) {
            valueName = ((BValue) value).getType().getName();
        } else {
            valueName = value.getClass().getName();
        }
        return new ApplicationError("Invalid parameter :" + valueName + " is passed as value for SQL type : "
                + sqlType);
    }



    public static String getString(Clob data) throws IOException, SQLException {
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

    public static BArray createTimeStruct(long millis) {
        return TimeValueHandler.createUtcFromMilliSeconds(millis);
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
        int count = statement.getUpdateCount();
        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put(AFFECTED_ROW_COUNT_FIELD, count);
        BMap<BString, Object> executionResult = ValueCreator.createRecordValue(
                ModuleUtils.getModule(), EXECUTION_RESULT_RECORD, resultFields);
        procedureCallResult.set(EXECUTION_RESULT_FIELD, executionResult);
    }

    public static void validatedInvalidFieldAssignment(int sqlType, Type type, String sqlTypeName)
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
                        throw new ApplicationError("The field '" + field.getKey() + "' of type " +
                                field.getValue().getFieldType().getName() + " cannot be mapped to the column '" +
                                columnName + "' of SQL type '" + sqlTypeName + "'");
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

    private static boolean isValidFieldConstraint(int sqlType, Type type) {
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
            case Types.TIME_WITH_TIMEZONE:
                return type.getTag() == TypeTags.STRING_TAG ||
                        type.getTag() == TypeTags.OBJECT_TYPE_TAG ||
                        type.getTag() == TypeTags.RECORD_TYPE_TAG ||
                        type.getTag() == TypeTags.INT_TAG;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return type.getTag() == TypeTags.STRING_TAG ||
                        type.getTag() == TypeTags.OBJECT_TYPE_TAG ||
                        type.getTag() == TypeTags.RECORD_TYPE_TAG ||
                        type.getTag() == TypeTags.INTERSECTION_TAG ||
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
                        type.getTag() == TypeTags.JSON_TAG ||
                        type.getTag() == TypeTags.RECORD_TYPE_TAG;
        }
    }

    public static BMap<BString, Object> createDateRecord(Date date) {
        LocalDate dateObj = ((Date) date).toLocalDate();
        BMap<BString, Object> dateMap = ValueCreator.createRecordValue(
                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD);
        dateMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_YEAR),
                dateObj.getYear());
        dateMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_MONTH),
                dateObj.getMonthValue());
        dateMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_DAY),
                dateObj.getDayOfMonth());
        return dateMap;
    }

    public static BMap<BString, Object> createTimeRecord(Time time) {
        LocalTime timeObj = ((Time) time).toLocalTime();
        BMap<BString, Object> timeMap = ValueCreator.createRecordValue(
                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                org.ballerinalang.stdlib.time.util.Constants.TIME_OF_DAY_RECORD);
        timeMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_HOUR), timeObj.getHour());
        timeMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_MINUTE) , timeObj.getMinute());
        BigDecimal second = new BigDecimal(timeObj.getSecond());
        second = second.add(new BigDecimal(timeObj.getNano())
                .divide(ANALOG_GIGA, MathContext.DECIMAL128));
        timeMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_SECOND), ValueCreator.createDecimalValue(second));
        return timeMap;
    }

    public static BMap<BString, Object> createTimeWithTimezoneRecord(java.time.OffsetTime offsetTime) {
        BMap<BString, Object> timeMap = ValueCreator.createRecordValue(
                                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                                org.ballerinalang.stdlib.time.util.Constants.TIME_OF_DAY_RECORD);
        timeMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_HOUR), offsetTime.getHour());
        timeMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_MINUTE), offsetTime.getMinute());
        BigDecimal second = new BigDecimal(offsetTime.getSecond());
        second = second.add(new BigDecimal(offsetTime.getNano()).divide(ANALOG_GIGA,
                MathContext.DECIMAL128));
        timeMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_SECOND), ValueCreator.createDecimalValue(second));
        Map<String, Integer> zoneInfo = TimeValueHandler
                .zoneOffsetMapFromString(offsetTime.getOffset().toString());
        BMap<BString, Object> zoneMap = ValueCreator.createRecordValue(
                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                org.ballerinalang.stdlib.time.util.Constants.READABLE_ZONE_OFFSET_RECORD);
        if (zoneInfo
            .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR) != null) {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR),
                    zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)
                            .longValue());
        } else {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR), 0);
        }
        if (zoneInfo
            .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE) != null) {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE),
                    zoneInfo
                        .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)
                        .longValue());
        } else {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE), 0);
        }
        if (zoneInfo
            .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND) != null) {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND),
                    zoneInfo
                    .get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)
                    .longValue());
        }
        zoneMap.freezeDirect();
        timeMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET), zoneMap);
        timeMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD_TIME_ABBREV),
                fromString(offsetTime.getOffset().toString()));
        return timeMap;
    }

    public static BMap<BString, Object> createTimestampRecord(Timestamp timestamp) {
        LocalDateTime dateTimeObj = ((Timestamp) timestamp).toLocalDateTime();
        BMap<BString, Object> civilMap = ValueCreator.createRecordValue(
                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD);
        civilMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_YEAR), dateTimeObj.getYear());
        civilMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_MONTH),
                dateTimeObj.getMonthValue());
        civilMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.DATE_RECORD_DAY),
                dateTimeObj.getDayOfMonth());
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_HOUR), dateTimeObj.getHour());
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_MINUTE), dateTimeObj.getMinute());
        BigDecimal second = new BigDecimal(dateTimeObj.getSecond());
        second = second.add(new BigDecimal(dateTimeObj.getNano())
                .divide(ANALOG_GIGA, MathContext.DECIMAL128));
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_SECOND), ValueCreator.createDecimalValue(second));
        return civilMap;
    }

    public static BMap<BString, Object> createTimestampWithTimezoneRecord(java.time.OffsetDateTime offsetDateTime) {
        BMap<BString, Object> civilMap = ValueCreator.createRecordValue(
            org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
            org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD);
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                        .DATE_RECORD_YEAR), offsetDateTime.getYear());
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                        .DATE_RECORD_MONTH), offsetDateTime.getMonthValue());
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                        .DATE_RECORD_DAY), offsetDateTime.getDayOfMonth());
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_HOUR), offsetDateTime.getHour());
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_MINUTE), offsetDateTime.getMinute());
        BigDecimal second = new BigDecimal(offsetDateTime.getSecond());
        second = second.add(new BigDecimal(offsetDateTime.getNano()).divide(ANALOG_GIGA,
                MathContext.DECIMAL128));
        civilMap.put(fromString(org.ballerinalang.stdlib.time.util.Constants
                .TIME_OF_DAY_RECORD_SECOND), ValueCreator.createDecimalValue(second));
        Map<String, Integer> zoneInfo = TimeValueHandler
                .zoneOffsetMapFromString(offsetDateTime.getOffset().toString());
        BMap<BString, Object> zoneMap = ValueCreator.createRecordValue(
                org.ballerinalang.stdlib.time.util.ModuleUtils.getModule(),
                org.ballerinalang.stdlib.time.util.Constants.READABLE_ZONE_OFFSET_RECORD);
        if (zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)
                != null) {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR),
                    zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR)
                            .longValue());
        } else {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_HOUR), 0);
        }
        if (zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)
                != null) {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE),
                    zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE)
                            .longValue());
        } else {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_MINUTE), 0);
        }
        if (zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)
                != null) {
            zoneMap.put(fromString(
                    org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND),
                    zoneInfo.get(org.ballerinalang.stdlib.time.util.Constants.ZONE_OFFSET_RECORD_SECOND)
                            .longValue());
        }
        zoneMap.freezeDirect();
        civilMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD_UTC_OFFSET), zoneMap);
        civilMap.put(fromString(
                org.ballerinalang.stdlib.time.util.Constants.CIVIL_RECORD_TIME_ABBREV),
                fromString(offsetDateTime.getOffset().toString()));
        return civilMap;
    }
}
