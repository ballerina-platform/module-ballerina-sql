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
                            int index, BObject typedValue)
            throws SQLException, ApplicationError, IOException;
    protected abstract Object[] getCustomArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getCustomStructData(Object value, Connection conn) 
            throws SQLException, ApplicationError;

    protected abstract void setVarchar(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException;
    protected abstract void setText(int index, Object value, PreparedStatement preparedStatement) throws SQLException;
    protected abstract void setChar(int index, Object value, PreparedStatement preparedStatement) throws SQLException;
    protected abstract void setNChar(int index, Object value, PreparedStatement preparedStatement) throws SQLException;
    protected abstract void setNVarchar(int index, Object value, PreparedStatement preparedStatement)
            throws SQLException;
    protected abstract void setBit(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setBoolean(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setInteger(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setBigInt(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setSmallInt(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setFloat(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setReal(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setDouble(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setNumeric(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setDecimal(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setBinary(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError, IOException;
    protected abstract void setVarBinary(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError, IOException;
    protected abstract void setBlob(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError, IOException;
    protected abstract void setClob(int index, Object value, PreparedStatement preparedStatement, String sqlType,
            Connection connection)
            throws SQLException, ApplicationError;
    protected abstract void setNClob(int index, Object value, PreparedStatement preparedStatement, String sqlType,
            Connection connection)
            throws SQLException, ApplicationError;
    protected abstract void setRow(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setStruct(int index, Object value, PreparedStatement preparedStatement,
            Connection connection)
            throws SQLException, ApplicationError;
    protected abstract void setRef(int index, Object value, PreparedStatement preparedStatement, Connection connection)
            throws SQLException, ApplicationError;
    protected abstract void setArray(int index, Object value, PreparedStatement preparedStatement,
            Connection connection)
            throws SQLException, ApplicationError;
    protected abstract void setDateTime(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setTimestamp(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setDate(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract void setTime(int index, Object value, PreparedStatement preparedStatement, String sqlType)
            throws SQLException, ApplicationError;
    protected abstract Object[] getIntArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getFloatArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getDecimalArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getStringArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getBooleanArrayData(Object value) throws ApplicationError;
    protected abstract Object[] getNestedArrayData(Object value) throws ApplicationError;

    protected abstract void getRecordStructData(Object bValue, Object[] structData, Connection conn, int i)
            throws SQLException, ApplicationError;
    protected abstract void getArrayStructData(Field field, Object bValue, Object[] structData, int i,
            String structuredSQLType)
            throws SQLException, ApplicationError;

}
