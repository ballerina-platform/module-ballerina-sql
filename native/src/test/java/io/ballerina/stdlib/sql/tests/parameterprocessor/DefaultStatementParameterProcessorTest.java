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
package io.ballerina.stdlib.sql.tests.parameterprocessor;

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultStatementParameterProcessor;
import io.ballerina.stdlib.sql.tests.TestUtils;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * DefaultStatementParameterProcessor class test.
 *
 * @since 0.6.0-beta.2
 */
public class DefaultStatementParameterProcessorTest {
    static class NullAndErrorCheckClass extends DefaultStatementParameterProcessor {
        void testGetCustomSQLType() throws ApplicationError, SQLException {
            getCustomSQLType(TestUtils.getMockObject("Object"));
        }

        void testSetCustomSqlTypedParam() throws SQLException, ApplicationError {
            setCustomSqlTypedParam(null , null, 0, TestUtils.getMockObject("Object"));
        }

        void testGetCustomArrayData(Object value) throws ApplicationError, SQLException {
            getCustomArrayData(value);
        }

        void testGetCustomStructData()throws SQLException, ApplicationError {
            getCustomStructData(TestUtils.getMockBValueJson());
        }

        void testSetBoolean() throws SQLException, ApplicationError {
            setBoolean(null, "Boolean", 0, TestUtils.getMockBValueJson());
        }

        void testSetNumericAndDecimal() throws SQLException, ApplicationError {
            setDecimal(null, "Decimal", 0, TestUtils.getMockBValueJson());
        }

        void testSetBinaryAndBlob() throws SQLException, ApplicationError {
            setBlob(null, "Blob", 0, TestUtils.getMockBValueJson());
        }

        void testSetClobAndNclob() throws SQLException, ApplicationError {
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
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getIntValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING), null, "Int Array");
        }

        void testGetDecimalValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getDecimalValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetRealValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getRealValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetNumericValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getNumericValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetDoubleValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getDoubleValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetFloatValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getFloatValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetDateValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getDateValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetTimeValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getTimeValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetBitValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getBitValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetBinaryValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getBinaryValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        void testGetDateTimeValueArrayData() throws ApplicationError {
            ArrayList<String> arrayList = new ArrayList<>();
            arrayList.add("new String");
            getDateTimeValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        Object[] testGetBitValueArrayDataBString() throws ApplicationError {
            ArrayList<BString> arrayList = new ArrayList<>();
            arrayList.add(fromString("True"));
            return getBitValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_STRING));
        }

        Object[] testGetBitValueArrayDataInteger(int i) throws ApplicationError {
            ArrayList<Integer> arrayList = new ArrayList<>();
            arrayList.add(i);
            return getBitValueArrayData(TestUtils.getBArray(arrayList, PredefinedTypes.TYPE_INT));
        }

        Object[] testGetRecordStructDataNull() throws SQLException, ApplicationError {
            Object[] array = new Object[1];
            getRecordStructData(TestUtils.getMockConnection(false), array, 0, null);
            return array;
        }

        Object[] testGetRecordStructDataRecord() throws SQLException, ApplicationError {
            Object[] array = new Object[1];
            getRecordStructData(TestUtils.getMockConnection(false), array, 0, TestUtils.getMockBMapRecord());
            return array;
        }
    }

    @Test
    void getCustomSQLTypeTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testGetCustomSQLType();
        } catch (ApplicationError | SQLException e) {
            assertEquals(e.getMessage(), "Unsupported SQL type: Object");
        }
    }

    @Test
    void setCustomSqlTypedParamTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testSetCustomSqlTypedParam();
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
            testClass.testGetCustomStructData();
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

    @Test
    void getBitValueArrayDataBStringTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object[] objects = testClass.testGetBitValueArrayDataBString();
            Boolean[] array = (Boolean[]) objects[0];
            assertEquals(array[0], true);
        } catch (Exception e) {
            fail("Exception received");
        }
    }

    @Test
    void getBitValueArrayDataIntegerTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object[] objects = testClass.testGetBitValueArrayDataInteger(1);
            Boolean[] array = (Boolean[]) objects[0];
            assertEquals(array[0], true);
        } catch (Exception e) {
            fail("Exception received");
        }
    }

    @Test
    void getBitValueArrayDataIntegerTest2() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object[] objects = testClass.testGetBitValueArrayDataInteger(12);
            Boolean[] array = (Boolean[]) objects[0];
            assertEquals(array[0], true);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Only 1 or 0 can be passed for BIT SQL Type, but found :12");
        }
    }

    @Test
    void getRecordStructDataNullTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object[] objects = testClass.testGetRecordStructDataNull();
            Struct struct = (Struct) objects[0];
            assertNull(struct.getSQLTypeName());
        } catch (Exception e) {
            fail("Exception received");
        }
    }

    @Test
    void getRecordStructDataRecordTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object[] objects = testClass.testGetRecordStructDataRecord();
            Struct struct = (Struct) objects[0];
            assertEquals(struct.getAttributes()[0], false);
        } catch (Exception e) {
            fail("Exception received");
        }
    }
}
