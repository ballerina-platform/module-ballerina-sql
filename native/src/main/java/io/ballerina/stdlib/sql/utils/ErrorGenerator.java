/*
 * Copyright (c) 2020, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * <p>
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.ballerina.stdlib.sql.utils;

import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.exception.ConversionError;
import io.ballerina.stdlib.sql.exception.DataError;
import io.ballerina.stdlib.sql.exception.FieldMismatchError;
import io.ballerina.stdlib.sql.exception.TypeMismatchError;
import io.ballerina.stdlib.sql.exception.UnsupportedTypeError;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for generating SQL Client errors.
 *
 * @since 1.2.0
 */
public class ErrorGenerator {
    private ErrorGenerator() {
    }

    public static BError getSQLBatchExecuteError(SQLException exception,
                                                 List<BMap<BString, Object>> executionResults,
                                                 String messagePrefix) {
        String sqlErrorMessage =
                exception.getMessage() != null ? exception.getMessage() : Constants.BATCH_EXECUTE_ERROR_MESSAGE;
        int vendorCode = exception.getErrorCode();
        String sqlState = exception.getSQLState();
        String errorMessage = messagePrefix + sqlErrorMessage + ".";
        return getSQLBatchExecuteError(errorMessage, vendorCode, sqlState, executionResults);
    }

    public static BError getSQLDatabaseError(SQLException exception, String messagePrefix) {
        String sqlErrorMessage =
                exception.getMessage() != null ? exception.getMessage() : Constants.DATABASE_ERROR_MESSAGE;
        int vendorCode = exception.getErrorCode();
        String sqlState = exception.getSQLState();
        String errorMessage = messagePrefix + sqlErrorMessage + ".";
        return getSQLDatabaseError(errorMessage, vendorCode, sqlState);
    }

    public static BError getSQLApplicationError(String errorMessage) {
        return ErrorCreator.createError(ModuleUtils.getModule(), Constants.APPLICATION_ERROR,
                StringUtils.fromString(errorMessage), null, null);
    }

    public static BError getSQLApplicationError(ApplicationError error) {
        String message = error.getMessage();
        if (message == null) {
            message = error.getClass().getName();
        }

        String errorName;
        if (error instanceof ConversionError) {
            errorName = Constants.CONVERSION_ERROR;
        } else if (error instanceof TypeMismatchError) {
            errorName = Constants.TYPE_MISMATCH_ERROR;
        } else if (error instanceof FieldMismatchError) {
            errorName = Constants.FIELD_MISMATCH_ERROR;
        } else if (error instanceof UnsupportedTypeError) {
            errorName = Constants.UNSUPPORTED_TYPE_ERROR;
        } else if (error instanceof DataError) {
            errorName = Constants.DATA_ERROR;
        } else {
            errorName = Constants.APPLICATION_ERROR;
        }
        return ErrorCreator.createError(ModuleUtils.getModule(), errorName,
                StringUtils.fromString(message), null, null);
    }

    public static BError getNoRowsError(String message) {
        return ErrorCreator.createError(ModuleUtils.getModule(), Constants.NO_ROWS_ERROR,
                StringUtils.fromString(message), null,  null);
    }

    public static BError getTypeMismatchError(String message) {
        return ErrorCreator.createError(ModuleUtils.getModule(), Constants.TYPE_MISMATCH_ERROR,
                StringUtils.fromString(message), null,  null);
    }

    private static BError getSQLBatchExecuteError(String message, int vendorCode, String sqlState,
                                                  List<BMap<BString, Object>> executionResults) {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(Constants.ErrorRecordFields.ERROR_CODE, vendorCode);
        valueMap.put(Constants.ErrorRecordFields.SQL_STATE, sqlState);
        valueMap.put(Constants.ErrorRecordFields.EXECUTION_RESULTS,
                ValueCreator.createArrayValue(executionResults.toArray(), TypeCreator.createArrayType(
                        TypeCreator.createRecordType(
                                Constants.EXECUTION_RESULT_RECORD, ModuleUtils.getModule(),
                                0, false, 0))));

        BMap<BString, Object> sqlClientErrorDetailRecord = ValueCreator.
                createRecordValue(ModuleUtils.getModule(), Constants.BATCH_EXECUTE_ERROR_DETAIL, valueMap);
        return ErrorCreator.createError(ModuleUtils.getModule(), Constants.BATCH_EXECUTE_ERROR,
                StringUtils.fromString(message), null, sqlClientErrorDetailRecord);
    }

    private static BError getSQLDatabaseError(String message, int vendorCode, String sqlState) {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(Constants.ErrorRecordFields.ERROR_CODE, vendorCode);
        valueMap.put(Constants.ErrorRecordFields.SQL_STATE, sqlState);
        BMap<BString, Object> sqlClientErrorDetailRecord = ValueCreator.
                createRecordValue(ModuleUtils.getModule(), Constants.DATABASE_ERROR_DETAILS, valueMap);
        return ErrorCreator.createError(ModuleUtils.getModule(), Constants.DATABASE_ERROR,
                StringUtils.fromString(message), null, sqlClientErrorDetailRecord);
    }
}
