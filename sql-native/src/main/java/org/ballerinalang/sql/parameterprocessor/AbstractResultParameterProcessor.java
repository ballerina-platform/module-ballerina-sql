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
package org.ballerinalang.sql.parameterprocessor;

import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.sql.utils.ColumnDefinition;

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
 * This class has abstract implementation of methods required convert SQL types into ballerina types and
 * other methods that process the parameters of the result.
 *
 * @since 0.5.6
 */
public abstract class AbstractResultParameterProcessor {

    protected abstract BArray createAndPopulateCustomValueArray(Object firstNonNullElement, Object[] dataArray)
            throws ApplicationError;

    protected abstract BArray createAndPopulateCustomBBRefValueArray(Object firstNonNullElement, Object[] dataArray,
                                                                     Type type) throws ApplicationError;

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

    protected abstract void populateChar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateLongVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateNChar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateNVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateLongNVarchar(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateVarBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateLongVarBinary(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateBlob(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateClob(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateNClob(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateDate(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateTime(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateTimeWithTimeZone(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateTimestamp(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateTimestampWithTimeZone(
            CallableStatement statement, BObject parameter, int paramIndex) throws SQLException;

    protected abstract void populateArray(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateRowID(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateTinyInt(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateSmallInt(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateInteger(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateBigInt(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateReal(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateFloat(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateDouble(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateNumeric(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateDecimal(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateBit(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateBoolean(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateRef(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateStruct(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    protected abstract void populateXML(CallableStatement statement, BObject parameter, int paramIndex)
            throws SQLException;

    public abstract void populateCustomOutParameters(
            CallableStatement statement, BObject parameter, int paramIndex, int sqlType) throws ApplicationError;

    protected abstract Object getCustomOutParameters(BObject value, int sqlType, Type ballerinaType);

    public abstract BObject createRecordIterator(
            ResultSet resultSet, Statement statement, Connection connection,
            List<ColumnDefinition> columnDefinitions, StructureType streamConstraint);
}
