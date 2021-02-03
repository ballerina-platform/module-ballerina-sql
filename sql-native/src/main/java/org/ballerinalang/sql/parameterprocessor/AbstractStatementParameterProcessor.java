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
import io.ballerina.runtime.api.values.BObject;
import org.ballerinalang.sql.exception.ApplicationError;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * This class has abstract implementation of methods required convert ballerina types into SQL types and
 * other methods that process the parameters of the statement.
 */
abstract class AbstractStatementParameterProcessor {

    protected abstract int getCustomOutParameterType(BObject typedValue) throws ApplicationError;
    protected abstract int getCustomSQLType(BObject typedValue) throws ApplicationError;
    protected abstract void setCustomSqlTypedParam(Connection connection, PreparedStatement preparedStatement,
            int index, BObject typedValue) throws SQLException, ApplicationError, IOException;
    protected abstract Object[] getCustomArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getCustomStructData(Connection conn, Object value) 
            throws SQLException, ApplicationError;
    protected abstract void setVarchar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException;
    protected abstract void setText(PreparedStatement preparedStatement, int index, Object value) throws SQLException;
    protected abstract void setChar(PreparedStatement preparedStatement, int index, Object value) throws SQLException;
    protected abstract void setNChar(PreparedStatement preparedStatement, int index, Object value) throws SQLException;
    protected abstract void setNVarchar(PreparedStatement preparedStatement, int index, Object value)
            throws SQLException;
    protected abstract void setBit(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setBoolean(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setInteger(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setBigInt(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setSmallInt(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setFloat(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setReal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setDouble(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setNumeric(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setDecimal(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setBinary(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError, IOException;
    protected abstract void setVarBinary(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError, IOException;
    protected abstract void setBlob(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError, IOException;
    protected abstract void setClob(Connection connection, PreparedStatement preparedStatement, String sqlType,
            int index, Object value) throws SQLException, ApplicationError;
    protected abstract void setNClob(Connection connection, PreparedStatement preparedStatement, String sqlType,
            int index, Object value) throws SQLException, ApplicationError;
    protected abstract void setRow(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setStruct(Connection connection, PreparedStatement preparedStatement, int index,
            Object value) throws SQLException, ApplicationError;
    protected abstract void setRef(Connection connection, PreparedStatement preparedStatement, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setArray(Connection connection, PreparedStatement preparedStatement, int index, 
            Object value) throws SQLException, ApplicationError;
    protected abstract void setDateTime(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setTimestamp(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setDate(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract void setTime(PreparedStatement preparedStatement, String sqlType, int index, Object value)
            throws SQLException, ApplicationError;
    protected abstract Object[] getIntArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getFloatArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getDecimalArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getStringArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getBooleanArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getNestedArrayData(Object value) throws ApplicationError;

    protected abstract void getRecordStructData(Connection conn, Object[] structData, int i, Object bValue)
            throws SQLException, ApplicationError;
    protected abstract void getArrayStructData(Field field, Object[] structData, String structuredSQLType, int i,
            Object bValue) throws SQLException, ApplicationError;

}
