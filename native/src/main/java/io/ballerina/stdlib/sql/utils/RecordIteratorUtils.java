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
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static io.ballerina.stdlib.sql.utils.Utils.cleanUpConnection;

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
                return resultParameterProcessor.processArrayResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                if (ballerinaType.getTag() == TypeTags.JSON_TAG) {
                    return resultParameterProcessor.processJsonResult(resultSet, columnIndex, sqlType, ballerinaType);
                } else {
                    return resultParameterProcessor.processCharResult(resultSet, columnIndex, sqlType, ballerinaType);
                }
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (ballerinaType.getTag() == TypeTags.STRING_TAG) {
                    return resultParameterProcessor.processCharResult(
                            resultSet, columnIndex, sqlType, ballerinaType, columnDefinition.getSqlName());
                } else {
                    return resultParameterProcessor.processByteArrayResult(
                            resultSet, columnIndex, sqlType, ballerinaType, columnDefinition.getSqlName());
                }
            case Types.BLOB:
                return resultParameterProcessor.processBlobResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.CLOB:
                return resultParameterProcessor.processClobResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.NCLOB:
                return resultParameterProcessor.processNClobResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.DATE:
                return resultParameterProcessor.processDateResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.TIME:
                return resultParameterProcessor.processTimeResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.TIME_WITH_TIMEZONE:
                return resultParameterProcessor.processTimeWithTimezoneResult(resultSet, columnIndex, sqlType,
                        ballerinaType);
            case Types.TIMESTAMP:
                return resultParameterProcessor.processTimestampResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return resultParameterProcessor.processTimestampWithTimezoneResult(resultSet, columnIndex, sqlType,
                        ballerinaType);
            case Types.ROWID:
                return resultParameterProcessor.processRowIdResult(resultSet, columnIndex, sqlType, ballerinaType,
                        "SQL RowID");
            case Types.TINYINT:
            case Types.SMALLINT:
                return resultParameterProcessor.processIntResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.INTEGER:
            case Types.BIGINT:
                return resultParameterProcessor.processLongResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.REAL:
            case Types.FLOAT:
                return resultParameterProcessor.processFloatResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.DOUBLE:
                return resultParameterProcessor.processDoubleResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.NUMERIC:
            case Types.DECIMAL:
                return resultParameterProcessor.processDecimalResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.BIT:
            case Types.BOOLEAN:
                return resultParameterProcessor.processBooleanResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.REF:
            case Types.STRUCT:
                return resultParameterProcessor.processStructResult(resultSet, columnIndex, sqlType, ballerinaType);
            case Types.SQLXML:
                return resultParameterProcessor.processXmlResult(resultSet, columnIndex, sqlType, ballerinaType);
            default:
                if (ballerinaType.getTag() == TypeTags.INT_TAG) {
                    resultParameterProcessor.processIntResult(resultSet, columnIndex, sqlType, ballerinaType);
                } else if (ballerinaType.getTag() == TypeTags.STRING_TAG
                        || ballerinaType.getTag() == TypeTags.ANY_TAG
                        || ballerinaType.getTag() == TypeTags.ANYDATA_TAG) {
                    return resultParameterProcessor.processCharResult(resultSet, columnIndex, sqlType, ballerinaType);
                } else if (ballerinaType.getTag() == TypeTags.BOOLEAN_TAG) {
                    return resultParameterProcessor.processBooleanResult(resultSet, columnIndex, sqlType,
                            ballerinaType);
                } else if (ballerinaType.getTag() == TypeTags.ARRAY_TAG &&
                        ((ArrayType) ballerinaType).getElementType().getTag() == TypeTags.BYTE_TAG) {
                    return resultParameterProcessor.processByteArrayResult(resultSet, columnIndex, sqlType,
                            ballerinaType, columnDefinition.getSqlName());
                } else if (ballerinaType.getTag() == TypeTags.FLOAT_TAG) {
                    return resultParameterProcessor.processDoubleResult(resultSet, columnIndex, sqlType,
                            ballerinaType);
                } else if (ballerinaType.getTag() == TypeTags.DECIMAL_TAG) {
                    return resultParameterProcessor.processDecimalResult(resultSet, columnIndex, sqlType,
                            ballerinaType);
                } else if (ballerinaType.getTag() == TypeTags.XML_TAG) {
                    return resultParameterProcessor.processXmlResult(resultSet, columnIndex, sqlType, ballerinaType);
                } else if (ballerinaType.getTag() == TypeTags.JSON_TAG) {
                    return resultParameterProcessor.processJsonResult(resultSet, columnIndex, sqlType, ballerinaType);
                }
                return resultParameterProcessor.processCustomTypeFromResultSet(resultSet, columnIndex,
                        columnDefinition);
        }
    }

    public static Object closeResult(BObject recordIterator) {
        ResultSet resultSet = (ResultSet) recordIterator.getNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD);
        Statement statement = (Statement) recordIterator.getNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD);
        Connection connection = (Connection) recordIterator.getNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD);
        return cleanUpConnection(recordIterator, resultSet, statement, connection);
    }
}
