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

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import io.ballerina.stdlib.sql.utils.ModuleUtils;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;

/**
 * This class has abstract implementation of methods to process JDBC Callable statement. Populate methods are used to
 * process callable statements and convert methods are used to transform the results to required ballerina types.
 *
 * @since 0.5.6
 */
public abstract class AbstractResultParameterProcessor {

    protected abstract BArray createAndPopulateCustomValueArray(Object firstNonNullElement, Type type,
            Array array) throws ApplicationError, SQLException;

    protected abstract BArray createAndPopulateCustomBBRefValueArray(Object firstNonNullElement, Type type,
            Array array) throws ApplicationError, SQLException;

    protected abstract void createUserDefinedTypeSubtype(Field internalField, StructureType structType)
            throws ApplicationError;

    protected abstract BArray convertArray(Array array, int sqlType, Type type)
            throws SQLException, ApplicationError;

    protected abstract BString convertChar(String value, int sqlType, Type type)
            throws ApplicationError;

    protected abstract Object convertChar(String value, int sqlType, Type type, String sqlTypeName)
            throws ApplicationError;

    protected abstract Object convertByteArray(byte[] value, int sqlType, Type type, String sqlTypeName)
            throws ApplicationError;

    protected abstract Object convertInteger(long value, int sqlType, Type type, boolean isNull)
            throws ApplicationError;

    protected abstract Object convertDouble(double value, int sqlType, Type type, boolean isNull)
            throws ApplicationError;

    protected abstract Object convertDecimal(BigDecimal value, int sqlType, Type type, boolean isNull)
            throws ApplicationError;

    protected abstract Object convertBlob(Blob value, int sqlType, Type type) throws ApplicationError, SQLException;

    protected abstract Object convertDate(java.util.Date date, int sqlType, Type type) throws ApplicationError;

    protected abstract Object convertTime(java.util.Date time, int sqlType, Type type) throws ApplicationError;

    protected abstract Object convertTimeWithTimezone(java.time.OffsetTime offsetTime, int sqlType, Type type)
            throws ApplicationError;

    protected abstract Object convertTimeStamp(java.util.Date timeStamp, int sqlType, Type type)
            throws ApplicationError;

    protected abstract Object convertTimestampWithTimezone(java.time.OffsetDateTime offsetDateTime, int sqlType,
                                                           Type type)throws ApplicationError;

    protected abstract Object convertBoolean(boolean value, int sqlType, Type type, boolean isNull)
            throws ApplicationError;

    public abstract Object convertBinary(Object value, int sqlType, Type ballerinaType) throws ApplicationError; 

    protected abstract Object convertStruct(Struct value, int sqlType, Type type) throws ApplicationError;

    protected abstract Object convertXml(SQLXML value, int sqlType, Type type) throws ApplicationError, SQLException;

    public abstract Object convertCustomOutParameter(Object value, String outParamObjectName, int sqlType,
                                                     Type ballerinaType);

    public abstract Object convertCustomInOutParameter(Object value, Object inParamValue, int sqlType,
                                                       Type ballerinaType);

    public abstract Object processChar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processVarchar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processLongVarchar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processNChar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processNVarchar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processLongNVarchar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBinary(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processVarBinary(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processLongVarBinary(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBlob(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processClob(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processNClob(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processDate(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTime(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTimeWithTimeZone(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTimestamp(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTimestampWithTimeZone(
            CallableStatement statement, int paramIndex) throws SQLException;

    public abstract Object processArray(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processRowID(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTinyInt(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processSmallInt(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processInteger(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBigInt(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processReal(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processFloat(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processDouble(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processNumeric(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processDecimal(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBit(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBoolean(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processRef(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processStruct(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processXML(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processCustomOutParameters(
            CallableStatement statement, int paramIndex, int sqlType) throws ApplicationError;

    public abstract Object processCustomTypeFromResultSet(ResultSet resultSet, int columnIndex,
                                                           ColumnDefinition columnDefinition) throws ApplicationError;

    public abstract Object convertArray(String objectTypeName, Object[] dataArray, Type ballerinaType);

    public BObject createRecordIterator(
            ResultSet resultSet, Statement statement, Connection connection,
            List<ColumnDefinition> columnDefinitions, StructureType streamConstraint) {
        BObject resultIterator = ValueCreator.createObjectValue(ModuleUtils.getModule(),
                Constants.RESULT_ITERATOR_OBJECT, null, getBalStreamResultIterator());
        resultIterator.addNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD, resultSet);
        resultIterator.addNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD, statement);
        resultIterator.addNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD, connection);
        resultIterator.addNativeData(Constants.COLUMN_DEFINITIONS_DATA_FIELD, columnDefinitions);
        resultIterator.addNativeData(Constants.RECORD_TYPE_DATA_FIELD, streamConstraint);
        return resultIterator;
    }

    public abstract BObject getBalStreamResultIterator();
}
