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

package io.ballerina.stdlib.sql.nativeimpl;

import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.AbstractResultParameterProcessor;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

import static io.ballerina.stdlib.sql.Constants.PARAMETER_INDEX_META_DATA;
import static io.ballerina.stdlib.sql.Constants.PROCEDURE_CALL_RESULT;
import static io.ballerina.stdlib.sql.Constants.REF_CURSOR_VALUE_NATIVE_DATA;
import static io.ballerina.stdlib.sql.Constants.RESULT_PARAMETER_PROCESSOR;
import static io.ballerina.stdlib.sql.Constants.STATEMENT_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.utils.Utils.getErrorStream;
import static io.ballerina.stdlib.sql.utils.Utils.getString;

/**
 * This class provides the implementation of processing InOut/Out parameters of procedure calls.
 *
 * @since 0.5.6
 */
public class OutParameterProcessor {

    private OutParameterProcessor() {
    }

    public static Object getOutParameterValue(BObject result, BTypedesc typeDesc) {
        return get(result, typeDesc, DefaultResultParameterProcessor.getInstance(), "OutParameter");
    }

    public static BStream getOutCursorValue(BObject result, BTypedesc typeDesc) {
        return get(result, typeDesc, DefaultResultParameterProcessor.getInstance());
    }

    public static Object getInOutParameterValue(BObject result, BTypedesc typeDesc) {
        return get(result, typeDesc, DefaultResultParameterProcessor.getInstance(), "InOutParameter");
    }

    private static Object populateOutParameter(BObject parameter) throws SQLException, ApplicationError {
        int paramIndex = (int) parameter.getNativeData(PARAMETER_INDEX_META_DATA);
        int sqlType = (int) parameter.getNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA);
        CallableStatement statement = (CallableStatement) parameter.getNativeData(STATEMENT_NATIVE_DATA_FIELD);
        BObject procedureCallResult = (BObject) parameter.getNativeData(PROCEDURE_CALL_RESULT);
        AbstractResultParameterProcessor resultParameterProcessor = (AbstractResultParameterProcessor) parameter
                .getNativeData(RESULT_PARAMETER_PROCESSOR);
        Object result = switch (sqlType) {
            case Types.CHAR -> resultParameterProcessor.processChar(statement, paramIndex);
            case Types.VARCHAR -> resultParameterProcessor.processVarchar(statement, paramIndex);
            case Types.LONGVARCHAR -> resultParameterProcessor.processLongVarchar(statement, paramIndex);
            case Types.NCHAR -> resultParameterProcessor.processNChar(statement, paramIndex);
            case Types.NVARCHAR -> resultParameterProcessor.processNVarchar(statement, paramIndex);
            case Types.LONGNVARCHAR -> resultParameterProcessor.processLongNVarchar(statement, paramIndex);
            case Types.BINARY -> resultParameterProcessor.processBinary(statement, paramIndex);
            case Types.VARBINARY -> resultParameterProcessor.processVarBinary(statement, paramIndex);
            case Types.LONGVARBINARY -> resultParameterProcessor.processLongVarBinary(statement, paramIndex);
            case Types.BLOB -> resultParameterProcessor.processBlob(statement, paramIndex);
            case Types.CLOB -> resultParameterProcessor.processClob(statement, paramIndex);
            case Types.NCLOB -> resultParameterProcessor.processNClob(statement, paramIndex);
            case Types.DATE -> resultParameterProcessor.processDate(statement, paramIndex);
            case Types.TIME -> resultParameterProcessor.processTime(statement, paramIndex);
            case Types.TIME_WITH_TIMEZONE -> resultParameterProcessor.processTimeWithTimeZone(statement, paramIndex);
            case Types.TIMESTAMP -> resultParameterProcessor.processTimestamp(statement, paramIndex);
            case Types.TIMESTAMP_WITH_TIMEZONE ->
                    resultParameterProcessor.processTimestampWithTimeZone(statement, paramIndex);
            case Types.ARRAY -> resultParameterProcessor.processArray(statement, paramIndex);
            case Types.ROWID -> resultParameterProcessor.processRowID(statement, paramIndex);
            case Types.TINYINT -> resultParameterProcessor.processTinyInt(statement, paramIndex);
            case Types.SMALLINT -> resultParameterProcessor.processSmallInt(statement, paramIndex);
            case Types.INTEGER -> resultParameterProcessor.processInteger(statement, paramIndex);
            case Types.BIGINT -> resultParameterProcessor.processBigInt(statement, paramIndex);
            case Types.REAL -> resultParameterProcessor.processReal(statement, paramIndex);
            case Types.FLOAT -> resultParameterProcessor.processFloat(statement, paramIndex);
            case Types.DOUBLE -> resultParameterProcessor.processDouble(statement, paramIndex);
            case Types.NUMERIC -> resultParameterProcessor.processNumeric(statement, paramIndex);
            case Types.DECIMAL -> resultParameterProcessor.processDecimal(statement, paramIndex);
            case Types.BIT -> resultParameterProcessor.processBit(statement, paramIndex);
            case Types.BOOLEAN -> resultParameterProcessor.processBoolean(statement, paramIndex);
            case Types.REF, Types.REF_CURSOR -> {
                Object output = resultParameterProcessor.processRef(statement, paramIndex);
                // This is to clean up the result set attached to the ref cursor out parameter
                // when procedure call result is closed.
                procedureCallResult.addNativeData(REF_CURSOR_VALUE_NATIVE_DATA, output);
                yield output;
            }
            case Types.STRUCT -> resultParameterProcessor.processStruct(statement, paramIndex);
            case Types.SQLXML -> resultParameterProcessor.processXML(statement, paramIndex);
            default -> resultParameterProcessor.processCustomOutParameters(statement, paramIndex, sqlType);
        };
        return result;
    }

    public static BStream get(BObject result, Object recordType,
                              AbstractResultParameterProcessor resultParameterProcessor) {
        Object value;
        try {
            value = populateOutParameter(result);
        } catch (SQLException | ApplicationError e) {
            return getErrorStream(recordType,
                    ErrorGenerator.getSQLError(e, "Failed to read parameter value."));
        }
        RecordType streamConstraint = (RecordType) TypeUtils.getReferredType(
                ((BTypedesc) recordType).getDescribingType());
        return resultParameterProcessor.convertCursorValue((ResultSet) value, streamConstraint);
    }

    public static Object get(BObject result, BTypedesc typeDesc,
                              AbstractResultParameterProcessor resultParameterProcessor, String parameterType) {
        int sqlType = (int) result.getNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA);
        Object value;
        try {
            value = populateOutParameter(result);
        } catch (SQLException | ApplicationError e) {
            return ErrorGenerator.getSQLError(e, "Failed to read parameter value.");
        }
        Type ballerinaType = TypeUtils.getReferredType(typeDesc.getDescribingType());
        try {
            if (ballerinaType.getTag() == TypeTags.UNION_TAG) {
                throw new ApplicationError(parameterType + " 'get' function does not support union return type.");
            }
            switch (sqlType) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    return resultParameterProcessor.convertChar((String) value, sqlType, ballerinaType);
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    return resultParameterProcessor.convertBinary(value, sqlType, ballerinaType);
                case Types.ARRAY:
                    Array array = (Array) value;
                    Object[] dataArray = (Object[]) array.getArray();
                    if (dataArray != null && dataArray.length != 0) {
                        String objectType = TypeUtils.getType(result).getName();
                        if (objectType.equals(Constants.ParameterObject.INOUT_PARAMETER) ||
                                objectType.equals(Constants.OutParameterTypes.ARRAY)) {
                            return resultParameterProcessor.convertArrayInOutParameter(dataArray, ballerinaType);
                        } else {
                            return resultParameterProcessor.convertArrayOutParameter(objectType, dataArray,
                                    ballerinaType);
                        }
                    }
                    return null;
                case Types.BLOB:
                    return resultParameterProcessor.convertBlob((Blob) value, sqlType, ballerinaType);
                case Types.CLOB:
                    String clobValue = getString((Clob) value, -1);
                    return resultParameterProcessor.convertChar(clobValue, sqlType, ballerinaType);
                case Types.NCLOB:
                    String nClobValue = getString((NClob) value, -1);
                    return resultParameterProcessor.convertChar(nClobValue, sqlType, ballerinaType);
                case Types.DATE:
                    return resultParameterProcessor.convertDate((Date) value, sqlType, ballerinaType);
                case Types.TIME:
                    return resultParameterProcessor.convertTime((Time) value, sqlType, ballerinaType);
                case Types.TIME_WITH_TIMEZONE:
                    return resultParameterProcessor.convertTimeWithTimezone((OffsetTime) value, sqlType,
                            ballerinaType);
                case Types.TIMESTAMP:
                    return resultParameterProcessor.convertTimeStamp((Timestamp) value, sqlType, ballerinaType);
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return resultParameterProcessor.convertTimestampWithTimezone((OffsetDateTime) value, sqlType,
                            ballerinaType);
                case Types.ROWID:
                    return resultParameterProcessor.convertByteArray(
                            ((RowId) value).getBytes(), sqlType, ballerinaType, "SQL RowID"
                    );
                case Types.TINYINT:
                case Types.SMALLINT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertInteger((int) value, sqlType, ballerinaType, false);
                case Types.INTEGER:
                case Types.BIGINT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertInteger((long) value, sqlType, ballerinaType, false);
                case Types.REAL:
                case Types.FLOAT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertDouble((float) value, sqlType, ballerinaType, false);
                case Types.DOUBLE:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertDouble((double) value, sqlType, ballerinaType, false);
                case Types.NUMERIC:
                case Types.DECIMAL:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertDecimal((BigDecimal) value, sqlType, ballerinaType, false);
                case Types.BIT:
                case Types.BOOLEAN:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertBoolean((boolean) value, sqlType, ballerinaType, false);
                case Types.REF:
                case Types.STRUCT:
                    return resultParameterProcessor.convertStruct((Struct) value, sqlType, ballerinaType);
                case Types.SQLXML:
                    return resultParameterProcessor.convertXml((SQLXML) value, sqlType, ballerinaType);
                default:
                    String objectType = TypeUtils.getType(result).getName();
                    if (objectType.equals(Constants.ParameterObject.INOUT_PARAMETER)) {
                        Object inParamValue = result.get(Constants.ParameterObject.IN_VALUE_FIELD);
                        return resultParameterProcessor.convertCustomInOutParameter(value, inParamValue, sqlType,
                                ballerinaType);
                    } else {
                        String outParamObjectName = TypeUtils.getType(result).getName();
                        return resultParameterProcessor.convertCustomOutParameter(value, outParamObjectName, sqlType,
                                ballerinaType);
                    }
            }
        } catch (ApplicationError applicationError) {
            return ErrorGenerator.getSQLApplicationError(applicationError);
        } catch (SQLException sqlException) {
            return ErrorGenerator.getSQLDatabaseError(sqlException, "Error when parsing out parameter.");
        } catch (Throwable th) {
            return ErrorGenerator.getSQLError(th, "Error when parsing out parameter.");
        }
    }
}
