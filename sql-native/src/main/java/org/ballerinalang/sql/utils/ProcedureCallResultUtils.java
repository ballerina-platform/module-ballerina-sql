/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.sql.utils;

import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BTypedesc;
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.sql.parameterprocessor.DefaultResultParameterProcessor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.ballerinalang.sql.Constants.EXECUTION_RESULT_FIELD;
import static org.ballerinalang.sql.Constants.QUERY_RESULT_FIELD;
import static org.ballerinalang.sql.Constants.RESULT_SET_COUNT_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.Constants.RESULT_SET_TOTAL_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.Constants.STATEMENT_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.Constants.TYPE_DESCRIPTIONS_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.utils.Utils.cleanUpConnection;
import static org.ballerinalang.sql.utils.Utils.getColumnDefinitions;
import static org.ballerinalang.sql.utils.Utils.getDefaultRecordType;
import static org.ballerinalang.sql.utils.Utils.updateProcedureCallExecutionResult;

/**
 * This class provides functionality for the `ProcedureCallResult` to iterate through the sql result sets.
 */
public class ProcedureCallResultUtils {

    public static Object getNextQueryResult(BObject procedureCallResult) {
        DefaultResultParameterProcessor resultParameterProcessor = DefaultResultParameterProcessor.getInstance();
        return getNextQueryResult(procedureCallResult, resultParameterProcessor);
    }

    public static Object getNextQueryResult(
            BObject procedureCallResult, DefaultResultParameterProcessor resultParameterProcessor) {
        CallableStatement statement = (CallableStatement) procedureCallResult
                .getNativeData(STATEMENT_NATIVE_DATA_FIELD);
        ResultSet resultSet;
        try {
            boolean moreResults = statement.getMoreResults();
            if (moreResults) {
                List<ColumnDefinition> columnDefinitions;
                StructureType streamConstraint;
                resultSet = statement.getResultSet();
                int totalRecordDescriptions = (int) procedureCallResult
                        .getNativeData(RESULT_SET_TOTAL_NATIVE_DATA_FIELD);
                if (totalRecordDescriptions == 0) {
                    columnDefinitions = getColumnDefinitions(resultSet, null);
                    streamConstraint = getDefaultRecordType(columnDefinitions);
                } else {
                    Object[] recordDescriptions = (Object[]) procedureCallResult
                            .getNativeData(TYPE_DESCRIPTIONS_NATIVE_DATA_FIELD);
                    int recordDescription = (int) procedureCallResult.getNativeData(RESULT_SET_COUNT_NATIVE_DATA_FIELD);
                    if (recordDescription <= totalRecordDescriptions) {
                        streamConstraint = (StructureType)
                                ((BTypedesc) recordDescriptions[recordDescription]).getDescribingType();
                        columnDefinitions = getColumnDefinitions(resultSet, streamConstraint);
                        procedureCallResult.addNativeData(RESULT_SET_COUNT_NATIVE_DATA_FIELD, recordDescription + 1);
                    } else {
                        throw new ApplicationError("The record description array count does not match with the " +
                                "returned result sets count.");
                    }
                }
                BStream streamValue = ValueCreator.createStreamValue(
                        TypeCreator.createStreamType(streamConstraint),
                        resultParameterProcessor.createRecordIterator(resultSet, null, null,
                                columnDefinitions, streamConstraint));
                procedureCallResult.set(QUERY_RESULT_FIELD, streamValue);
                procedureCallResult.set(EXECUTION_RESULT_FIELD, null);
            } else {
                updateProcedureCallExecutionResult(statement, procedureCallResult);
            }
            return moreResults;
        } catch (SQLException e) {
            return ErrorGenerator.getSQLDatabaseError(e, "Error when accessing the next query result.");
        } catch (ApplicationError e) {
            return ErrorGenerator.getSQLApplicationError("Error when accessing the next query result. "
                    + e.getMessage());
        } catch (Throwable throwable) {
            return ErrorGenerator.getSQLApplicationError("Error when accessing the next SQL result. "
                    + throwable.getMessage());
        }
    }

    public static Object closeCallResult(BObject procedureCallResult) {
        Statement statement = (Statement) procedureCallResult.getNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD);
        Connection connection = (Connection) procedureCallResult.getNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD);
        return cleanUpConnection(procedureCallResult, null, statement, connection);
    }
}
