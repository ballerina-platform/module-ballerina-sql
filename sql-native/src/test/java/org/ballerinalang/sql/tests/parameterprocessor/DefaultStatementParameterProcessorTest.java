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
package org.ballerinalang.sql.tests.parameterprocessor;

import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.sql.parameterprocessor.DefaultStatementParameterProcessor;
import org.ballerinalang.sql.tests.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DefaultStatementParameterProcessor class test.
 *
 * @since 0.6.0-beta.2
 */
public class DefaultStatementParameterProcessorTest {
    static class NullAndErrorCheckClass extends DefaultStatementParameterProcessor {
        int testGetCustomSQLType(String name) throws ApplicationError {
            return getCustomSQLType(TestUtils.getMockObject(name));
        }

        void testSetCustomSqlTypedParam(String name) throws SQLException, ApplicationError, IOException {
            setCustomSqlTypedParam(null , null, 0, TestUtils.getMockObject(name));
        }

        Object[] testGetCustomArrayData(Object value) throws ApplicationError {
            return getCustomArrayData(value);
        }

        Object[] testGetCustomStructData(Object value)throws SQLException, ApplicationError {
            return getCustomStructData(null, TestUtils.getMockBValueJson());
        }

        void testSetBoolean() throws SQLException, ApplicationError {
            setBoolean(null, "Boolean", 0, TestUtils.getMockBValueJson());
        }

        void testSetNumericAndDecimal() throws SQLException, ApplicationError {
            setDecimal(null, "Decimal", 0, TestUtils.getMockBValueJson());
        }

        void testSetBinaryAndBlob() throws SQLException, ApplicationError, IOException {
            setBlob(null, "Blob", 0, TestUtils.getMockBValueJson());
        }

        void testSetClobAndNclob() throws SQLException, ApplicationError, IOException {
            setClob(TestUtils.getMockConnection(false), null, "Clob", 0, TestUtils.getMockObject("Object"));
        }

        void testSetInteger() throws SQLException, ApplicationError {
            setInteger(null, "Int", 0, TestUtils.getMockBValueJson());
        }

        void testSetBigInt() throws SQLException, ApplicationError {
            setBigInt(null, "BigInt", 0, TestUtils.getMockBValueJson());
        }

        void testSetSmallInt() throws SQLException, ApplicationError {
            setSmallInt(null, "SmallInt", 0, TestUtils.getMockBValueJson());
        }

        void testSetFloat() throws SQLException, ApplicationError {
            setFloat(null, "Float", 0, TestUtils.getMockBValueJson());
        }

        void testSetReal() throws SQLException, ApplicationError {
            setReal(null, "Real", 0, TestUtils.getMockBValueJson());
        }

        void testSetDouble() throws SQLException, ApplicationError {
            setDouble(null, "Double", 0, TestUtils.getMockBValueJson());
        }

        void testSetRow() throws SQLException, ApplicationError {
            setRow(null, "Row", 0, TestUtils.getMockBValueJson());
        }

        void testSetDate() throws SQLException, ApplicationError {
            setDate(null, "Date", 0, TestUtils.getMockBValueJson());
        }

        void testSetTime() throws SQLException, ApplicationError {
            setTime(null, "Time", 0, TestUtils.getMockBValueJson());
        }

        void testGetIntValueArrayData() throws ApplicationError {
             getIntValueArrayData(TestUtils.getBArray(), null, "Int Array");
        }

        void testGetDecimalValueArrayData() throws ApplicationError {
            getDecimalValueArrayData(TestUtils.getBArray());
        }

        void testGetRealValueArrayData() throws ApplicationError {
            getRealValueArrayData(TestUtils.getBArray());
        }

        void testGetNumericValueArrayData() throws ApplicationError {
            getNumericValueArrayData(TestUtils.getBArray());
        }

        void testGetDoubleValueArrayData() throws ApplicationError {
            getDoubleValueArrayData(TestUtils.getBArray());
        }

        void testGetFloatValueArrayData() throws ApplicationError {
            getFloatValueArrayData(TestUtils.getBArray());
        }

        void testGetDateValueArrayData() throws ApplicationError {
            getDateValueArrayData(TestUtils.getBArray());
        }

        void testGetTimeValueArrayData() throws ApplicationError {
            getTimeValueArrayData(TestUtils.getBArray());
        }

        void testGetBitValueArrayData() throws ApplicationError {
            getBitValueArrayData(TestUtils.getBArray());
        }

        void testGetBinaryValueArrayData() throws ApplicationError, IOException {
            getBinaryValueArrayData(TestUtils.getBArray());
        }

        void testGetDateTimeValueArrayData() throws ApplicationError {
            getDateTimeValueArrayData(TestUtils.getBArray());
        }
    }

    @Test
    void getCustomSQLTypeTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetCustomSQLType("Object");
        } catch (ApplicationError e) {
            assertEquals(e.getMessage(), "Unsupported SQL type: Object");
        }
    }

    @Test
    void setCustomSqlTypedParamTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetCustomSqlTypedParam("Object");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Unsupported SQL type: Object");
        }
    }

    @Test
    void getCustomArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetCustomArrayData(TestUtils.getMockBValueJson());
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : ArrayValue");
        }
    }

    @Test
    void getCustomStructDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetCustomStructData(TestUtils.getMockBValueJson());
        } catch (Exception e) {
            assertEquals(e.getMessage(), "unsupported data type of JSON specified for struct parameter");
        }
    }

    @Test
    void setBooleanTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetBoolean();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Boolean");
        }
    }

    @Test
    void setNumericAndDecimalTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetNumericAndDecimal();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Decimal");
        }
    }

    @Test
    void setBinaryAndBlobTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetBinaryAndBlob();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Blob");
        }
    }

    @Test
    void setClobAndNclobTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetClobAndNclob();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :Object is passed as value for SQL type : Clob");
        }
    }

    @Test
    void setIntegerTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetInteger();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Int");
        }
    }

    @Test
    void setBigIntTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetBigInt();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : BigInt");
        }
    }

    @Test
    void setSmallIntTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetSmallInt();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : SmallInt");
        }
    }

    @Test
    void setFloatTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetFloat();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Float");
        }
    }

    @Test
    void setRealTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetReal();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Real");
        }
    }

    @Test
    void setDoubleTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetDouble();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Double");
        }
    }

    @Test
    void setRowTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetRow();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Row");
        }
    }

    @Test
    void setDateTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetDate();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Date");
        }
    }

    @Test
    void setTimeTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetTime();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :json is passed as value for SQL type : Time");
        }
    }

    @Test
    void getIntValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetIntValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : Int Array");
        }
    }

    @Test
    void getDecimalValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetDecimalValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : Decimal Array");
        }
    }

    @Test
    void getRealValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetRealValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : Real Array");
        }
    }

    @Test
    void getNumericValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetNumericValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : Numeric Array");
        }
    }

    @Test
    void getDoubleValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetDoubleValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : Double Array");
        }
    }

    @Test
    void getFloatValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetFloatValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : Float Array");
        }
    }

    @Test
    void getDateValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetDateValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : Date Array");
        }
    }

    @Test
    void getTimeValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetTimeValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : Time Array");
        }
    }

    @Test
    void getBitValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetBitValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : BIT Array");
        }
    }

    @Test
    void getBinaryValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetBinaryValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :java.lang.String is passed as value for SQL " +
                    "type : BINARY");
        }
    }

    @Test
    void getDateTimeValueArrayDataTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetDateTimeValueArrayData();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Invalid parameter :string is passed as value for SQL " +
                    "type : TIMESTAMP ARRAY");
        }
    }
}
