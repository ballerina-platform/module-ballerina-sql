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

    protected abstract Object convertCustomOutParameters(BObject value, int sqlType, Type ballerinaType);

    public abstract void processChar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processLongVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processNChar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processNVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processLongNVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processVarBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processLongVarBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processBlob(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processClob(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processNClob(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processDate(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processTime(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processTimeWithTimeZone(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processTimestamp(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processTimestampWithTimeZone(
            CallableStatement statement, BObject parameter, int paramIndex) throws SQLException;

    public abstract void processArray(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processRowID(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processTinyInt(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processSmallInt(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processInteger(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processBigInt(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processReal(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processFloat(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processDouble(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processNumeric(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processDecimal(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processBit(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processBoolean(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processRef(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processStruct(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processXML(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void processCustomOutParameters(
            CallableStatement statement, BObject parameter, int paramIndex, int sqlType) throws ApplicationError;

    public abstract Object processCustomTypeFromResultSet(ResultSet resultSet, int columnIndex,
                                                           ColumnDefinition columnDefinition) throws ApplicationError;

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
