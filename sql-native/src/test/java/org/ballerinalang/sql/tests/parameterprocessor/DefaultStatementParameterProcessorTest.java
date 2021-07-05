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

        void testSetInteger() throws SQLException, ApplicationError {
            setInteger(null, "Int", 0, TestUtils.getMockBValueJson());
        }

        void testSetBigInt() throws SQLException, ApplicationError {
            setBigInt(null, "BigInt", 0, TestUtils.getMockBValueJson());
        }

        void testSetFloat() throws SQLException, ApplicationError {
            setFloat(null, "Float", 0, TestUtils.getMockBValueJson());
        }

        void testSetReal() throws SQLException, ApplicationError {
            setFloat(null, "Real", 0, TestUtils.getMockBValueJson());
        }

        void testSetDouble() throws SQLException, ApplicationError {
            setDouble(null, "Double", 0, TestUtils.getMockBValueJson());
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
}
