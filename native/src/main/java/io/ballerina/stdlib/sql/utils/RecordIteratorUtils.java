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
package io.ballerina.stdlib.sql.utils;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.JsonUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;

/**
 * This class provides functionality for the `RecordIterator` to iterate through the sql result set.
 *
 * @since 1.2.0
 */
public class RecordIteratorUtils {
    public RecordIteratorUtils() {
    }

    public static Object nextResult(BObject recordIterator) {
        DefaultResultParameterProcessor resultParameterProcessor = DefaultResultParameterProcessor.getInstance();
        return nextResult(recordIterator, resultParameterProcessor);
    }

    public static Object nextResult(BObject recordIterator, DefaultResultParameterProcessor resultParameterProcessor) {
        ResultSet resultSet = (ResultSet) recordIterator.getNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD);
        try {
            if (resultSet.next()) {
                StructureType streamConstraint = (StructureType) recordIterator.
                        getNativeData(Constants.RECORD_TYPE_DATA_FIELD);
                BMap<BString, Object> bStruct = ValueCreator.createMapValue(streamConstraint);
                List<ColumnDefinition> columnDefinitions = (List<ColumnDefinition>) recordIterator
                        .getNativeData(Constants.COLUMN_DEFINITIONS_DATA_FIELD);
                for (int i = 0; i < columnDefinitions.size(); i++) {
                    ColumnDefinition columnDefinition = columnDefinitions.get(i);
                    bStruct.put(fromString(columnDefinition.getBallerinaFieldName()),
                            getResult(resultSet, i + 1, columnDefinition, resultParameterProcessor));
                }
                return bStruct;
            } else {
                return null;
            }
        } catch (SQLException e) {
            return ErrorGenerator.getSQLDatabaseError(e, "Error when iterating the SQL result");
        } catch (IOException | ApplicationError e) {
            return ErrorGenerator.getSQLApplicationError("Error when iterating the SQL result. "
                    + e.getMessage());
        } catch (Throwable throwable) {
            return ErrorGenerator.getSQLApplicationError("Error when iterating through the " +
                    "SQL result. " + throwable.getMessage());
        }
    }

    private static Object getResult(ResultSet resultSet, int columnIndex, ColumnDefinition columnDefinition,
                    DefaultResultParameterProcessor resultParameterProcessor)
            throws SQLException, ApplicationError, IOException {
        int sqlType = columnDefinition.getSqlType();
        Type ballerinaType = columnDefinition.getBallerinaType();
        switch (sqlType) {
            case Types.ARRAY:
                return resultParameterProcessor.convertArray(resultSet.getArray(columnIndex), sqlType, ballerinaType);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                if (ballerinaType.getTag() == TypeTags.JSON_TAG) {
                    return getJson(resultSet, columnIndex, sqlType, ballerinaType, resultParameterProcessor);
                } else {
                    return resultParameterProcessor.convertChar(resultSet.getString(columnIndex),
                            sqlType, ballerinaType);
                }
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (ballerinaType.getTag() == TypeTags.STRING_TAG) {
                    return resultParameterProcessor.convertChar(
                            resultSet.getString(columnIndex), sqlType, ballerinaType, columnDefinition.getSqlName());
                } else {
                    return resultParameterProcessor.convertByteArray(
                            resultSet.getBytes(columnIndex), sqlType, ballerinaType, columnDefinition.getSqlName());
                }
            case Types.BLOB:
                return resultParameterProcessor.convertBlob(resultSet.getBlob(columnIndex), sqlType, ballerinaType);
            case Types.CLOB:
                String clobValue = Utils.getString(resultSet.getClob(columnIndex));
                return resultParameterProcessor.convertChar(clobValue, sqlType, ballerinaType);
            case Types.NCLOB:
                String nClobValue = Utils.getString(resultSet.getNClob(columnIndex));
                return resultParameterProcessor.convertChar(nClobValue, sqlType, ballerinaType);
            case Types.DATE:
                Date date = resultSet.getDate(columnIndex);
                return resultParameterProcessor.convertDate(date, sqlType, ballerinaType);
            case Types.TIME:
                Time time = resultSet.getTime(columnIndex);
                return resultParameterProcessor.convertTime(time, sqlType, ballerinaType);
            case Types.TIME_WITH_TIMEZONE:
                OffsetTime offsetTime = resultSet.getObject(columnIndex, OffsetTime.class);
                return resultParameterProcessor.convertTimeWithTimezone(offsetTime, sqlType, ballerinaType);
            case Types.TIMESTAMP:
                Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                return resultParameterProcessor.convertTimeStamp(timestamp, sqlType, ballerinaType);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
                return resultParameterProcessor.convertTimestampWithTimezone(offsetDateTime, sqlType, ballerinaType);
            case Types.ROWID:
                return resultParameterProcessor.convertByteArray(resultSet.getRowId(columnIndex).getBytes(), sqlType, 
                            ballerinaType, "SQL RowID");
            case Types.TINYINT:
            case Types.SMALLINT:
                long iValue = resultSet.getInt(columnIndex);
                return resultParameterProcessor.convertInteger(iValue, sqlType, ballerinaType, resultSet.wasNull());
            case Types.INTEGER:
            case Types.BIGINT:
                long lValue = resultSet.getLong(columnIndex);
                return resultParameterProcessor.convertInteger(lValue, sqlType, ballerinaType, resultSet.wasNull());
            case Types.REAL:
            case Types.FLOAT:
                double fValue = resultSet.getFloat(columnIndex);
                return resultParameterProcessor.convertDouble(fValue, sqlType, ballerinaType, resultSet.wasNull());
            case Types.DOUBLE:
                double dValue = resultSet.getDouble(columnIndex);
                return resultParameterProcessor.convertDouble(dValue, sqlType, ballerinaType, resultSet.wasNull());
            case Types.NUMERIC:
            case Types.DECIMAL:
                BigDecimal decimalValue = resultSet.getBigDecimal(columnIndex);
                return resultParameterProcessor.convertDecimal(
                        decimalValue, sqlType, ballerinaType, resultSet.wasNull());
            case Types.BIT:
            case Types.BOOLEAN:
                boolean boolValue = resultSet.getBoolean(columnIndex);
                return resultParameterProcessor.convertBoolean(boolValue, sqlType, ballerinaType, resultSet.wasNull());
            case Types.REF:
            case Types.STRUCT:
                Struct structData = (Struct) resultSet.getObject(columnIndex);
                return resultParameterProcessor.convertStruct(structData, sqlType, ballerinaType);
            case Types.SQLXML:
                SQLXML sqlxml = resultSet.getSQLXML(columnIndex);
                return resultParameterProcessor.convertXml(sqlxml, sqlType, ballerinaType);
            default:
                if (ballerinaType.getTag() == TypeTags.INT_TAG) {
                    resultParameterProcessor.convertInteger(
                            resultSet.getInt(columnIndex), sqlType, ballerinaType, resultSet.wasNull());
                } else if (ballerinaType.getTag() == TypeTags.STRING_TAG
                        || ballerinaType.getTag() == TypeTags.ANY_TAG
                        || ballerinaType.getTag() == TypeTags.ANYDATA_TAG) {
                    return resultParameterProcessor.convertChar(
                            resultSet.getString(columnIndex), sqlType, ballerinaType);
                } else if (ballerinaType.getTag() == TypeTags.BOOLEAN_TAG) {
                    return resultParameterProcessor.convertBoolean(
                            resultSet.getBoolean(columnIndex), sqlType, ballerinaType, resultSet.wasNull());
                } else if (ballerinaType.getTag() == TypeTags.ARRAY_TAG &&
                        ((ArrayType) ballerinaType).getElementType().getTag() == TypeTags.BYTE_TAG) {
                    return resultParameterProcessor.convertByteArray(
                            resultSet.getBytes(columnIndex), sqlType, ballerinaType, columnDefinition.getSqlName());
                } else if (ballerinaType.getTag() == TypeTags.FLOAT_TAG) {
                    return resultParameterProcessor.convertDouble(
                            resultSet.getDouble(columnIndex), sqlType, ballerinaType, resultSet.wasNull());
                } else if (ballerinaType.getTag() == TypeTags.DECIMAL_TAG) {
                    return resultParameterProcessor.convertDecimal(
                            resultSet.getBigDecimal(columnIndex), sqlType, ballerinaType, resultSet.wasNull());
                } else if (ballerinaType.getTag() == TypeTags.XML_TAG) {
                    return resultParameterProcessor.convertXml(
                            resultSet.getSQLXML(columnIndex), sqlType, ballerinaType);
                } else if (ballerinaType.getTag() == TypeTags.JSON_TAG) {
                    return getJson(resultSet, columnIndex, sqlType, ballerinaType, resultParameterProcessor);
                }
                return resultParameterProcessor.getCustomResult(resultSet, columnIndex, columnDefinition);
        }
    }

    public static Object getJson(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType,
                                 DefaultResultParameterProcessor resultParameterProcessor)
            throws ApplicationError, SQLException {
        String jsonString = resultParameterProcessor.convertChar(
                resultSet.getString(columnIndex), sqlType, ballerinaType).getValue();
        Reader reader = new StringReader(jsonString);
        try {
            return JsonUtils.parse(reader, JsonUtils.NonStringValueProcessingMode.FROM_JSON_STRING);
        } catch (BError e) {
            throw new ApplicationError("Error while converting to JSON type. " + e.getDetails());
        }
    }

    public static Object closeResult(BObject recordIterator) {
        ResultSet resultSet = (ResultSet) recordIterator.getNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD);
        Statement statement = (Statement) recordIterator.getNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD);
        Connection connection = (Connection) recordIterator.getNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD);
        return Utils.cleanUpConnection(recordIterator, resultSet, statement, connection);
    }
}
