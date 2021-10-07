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

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
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
                StructureType streamConstraint = (StructureType) recordIterator.
                        getNativeData(Constants.RECORD_TYPE_DATA_FIELD);
                BMap<BString, Object> bStruct = ValueCreator.createMapValue(streamConstraint);
                List<ColumnDefinition> columnDefinitions = (List<ColumnDefinition>) recordIterator
                        .getNativeData(Constants.COLUMN_DEFINITIONS_DATA_FIELD);
                Utils.updateBallerinaRecordFields(resultParameterProcessor, resultSet, bStruct, columnDefinitions);
                return bStruct;
            } else {
                return null;
            }
        } catch (SQLException e) {
            return ErrorGenerator.getSQLDatabaseError(e, "Error when iterating the SQL result");
        } catch (ApplicationError e) {
            return ErrorGenerator.getSQLApplicationError("Error when iterating the SQL result. "
                    + e.getMessage());
        } catch (Throwable throwable) {
            return ErrorGenerator.getSQLApplicationError("Error when iterating through the " +
                    "SQL result. " + throwable.getMessage());
        }
    }

    public static Object closeResult(BObject recordIterator) {
        ResultSet resultSet = (ResultSet) recordIterator.getNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD);
        Statement statement = (Statement) recordIterator.getNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD);
        Connection connection = (Connection) recordIterator.getNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD);
        return cleanUpConnection(recordIterator, resultSet, statement, connection);
    }
}
