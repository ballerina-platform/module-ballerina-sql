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

import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
                RecordType streamConstraint = (RecordType) recordIterator.getNativeData(
                        Constants.RECORD_TYPE_DATA_FIELD);
                List<ColumnDefinition> columnDefinitions = (List<ColumnDefinition>) recordIterator
                        .getNativeData(Constants.COLUMN_DEFINITIONS_DATA_FIELD);
                return Utils.createBallerinaRecord(streamConstraint, resultParameterProcessor, resultSet,
                        columnDefinitions);
            }
            // Stream has reached the end, we clean up the resources, here any error from closing the stream is ignored.
            closeResult(recordIterator);
            return null;
        } catch (SQLException e) {
            // Stream throws an error, we clean up the resources, here any error from closing the stream is ignored.
            closeResult(recordIterator);
            return ErrorGenerator.getSQLDatabaseError(e, "Error when iterating the SQL result");
        } catch (ApplicationError e) {
            // Stream throws an error, we clean up the resources, here any error from closing the stream is ignored.
            closeResult(recordIterator);
            return ErrorGenerator.getSQLApplicationError(e, "Error when iterating the SQL result. ");
        } catch (Throwable throwable) {
            // Stream throws an error, we clean up the resources, here any error from closing the stream is ignored.
            closeResult(recordIterator);
            return ErrorGenerator.getSQLApplicationError("Error when iterating through the " +
                    "SQL result. " + throwable.getMessage());
        }
    }

    public static Object closeResult(BObject recordIterator) {
        ResultSet resultSet = (ResultSet) recordIterator.getNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD);
        Statement statement = (Statement) recordIterator.getNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD);
        Connection connection = (Connection) recordIterator.getNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD);
        boolean isWithinTrxBlock = (boolean) recordIterator.getNativeData(Constants.IS_IN_TRX);
        return cleanUpConnection(recordIterator, resultSet, statement, connection, isWithinTrxBlock);
    }
}
