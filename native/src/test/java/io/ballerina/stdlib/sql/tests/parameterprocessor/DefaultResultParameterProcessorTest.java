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
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.internal.values.MapValueImpl;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;
import io.ballerina.stdlib.sql.tests.TestUtils;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Date;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * DefaultResultParameterProcessor class test.
 *
 * @since 0.6.0-beta.2
 */
public class DefaultResultParameterProcessorTest {

    static class NullAndErrorCheckClass extends DefaultResultParameterProcessor {
        BArray testNullCreateAndPopulateCustomBBRefValueArray() throws ApplicationError, SQLException {
            return createAndPopulateCustomBBRefValueArray(null, null, null);
        }

        BMap<BString, Object> testNullCreateUserDefinedType() throws ApplicationError {
            return createUserDefinedType(null, null);
        }

        BArray testCreateAndPopulateCustomValueArray() throws ApplicationError, SQLException {
            return createAndPopulateCustomValueArray(null, null, null);
        }

        void testCreateUserDefinedTypeSubtype() throws ApplicationError {
            createUserDefinedTypeSubtype(TestUtils.getField(), PredefinedTypes.STRING_ITR_NEXT_RETURN_TYPE);
        }

        Object testConvertChar() throws ApplicationError {
            return convertChar("4", 4, PredefinedTypes.TYPE_INT, "INTEGER");
        }
    }

    @Test
    void createAndPopulateCustomBBRefValueArrayTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.testNullCreateAndPopulateCustomBBRefValueArray(), "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void createUserDefinedTypeTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.testNullCreateUserDefinedType(), "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void getCustomResultTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        ColumnDefinition columnDefinition = new TestUtils.ExtendedColumnDefinition("int_type", null,
                2, "INT", TypeUtils.getType(1), false);
        try {
            testClass.processCustomTypeFromResultSet(null, 1, columnDefinition);
        } catch (ApplicationError | SQLException e) {
            assertEquals(e.getMessage(), "Unsupported SQL type INT");
        }
    }

    @Test
    void populateCustomOutParametersTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.processCustomOutParameters(null, 2, 3);
        } catch (ApplicationError e) {
            assertEquals(e.getMessage(), "Unsupported SQL type '3' when reading Procedure call Out" +
                    " parameter of index '2'.");
        }
    }

    @Test
    void getCustomOutParametersTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        BObject bObject = TestUtils.getMockObject("ObjectType");
        try {
            Object object = testClass.convertCustomOutParameter(bObject, "", 1, null);
        } catch (NullPointerException e) {
            assertEquals(e.getMessage(), null);
        }
    }

    @Test
    void convertXmlTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.convertXml(null, Types.SQLXML, PredefinedTypes.TYPE_XML)
                    , "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.convertStruct(null, Types.STRUCT, PredefinedTypes.STRING_ITR_NEXT_RETURN_TYPE)
                    , "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructErrorTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.convertStruct(TestUtils.getStruct(), Types.INTEGER, PredefinedTypes.TYPE_INT);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "The ballerina type that can be used for SQL struct should be record type," +
                    " but found int .");
        }
    }

    @Test
    void convertStructTest1() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getStruct(), Types.STRUCT,
                    PredefinedTypes.STRING_ITR_NEXT_RETURN_TYPE);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "specified record and the returned SQL Struct field counts are " +
                    "different, and hence not compatible");
        }
    }

    @Test
    void convertStructTest2() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getIntStruct(),
                    Types.STRUCT, TestUtils.getIntStructRecord());
            BMap<BString, Object> map = (BMap<BString, Object>) object;
            assertEquals(map.get(fromString("value1")), 2);
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructTest3() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getDecimalStruct(),
                    Types.STRUCT, TestUtils.getIntStructRecord());
            BMap<BString, Object> map = (BMap<BString, Object>) object;
            assertEquals(map.get(fromString("value1")), 2);
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructTest4() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getBooleanStruct(),
                    Types.STRUCT, TestUtils.getBooleanStructRecord());
            BMap<BString, Object> map = (BMap<BString, Object>) object;
            assertEquals(map.get(fromString("value1")), false);
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructTest5() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getFloatStruct(),
                    Types.STRUCT, TestUtils.getFloatStructRecord());
            BMap<BString, Object> map = (BMap<BString, Object>) object;
            assertEquals(map.get(fromString("value1")), 2.3);
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructTest6() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getDecimalStruct(),
                    Types.STRUCT, TestUtils.getFloatStructRecord());
            BMap<BString, Object> map = (BMap<BString, Object>) object;
            assertEquals(map.get(fromString("value1")), 2.0);
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructTest7() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getDecimalStruct(),
                    Types.STRUCT, TestUtils.getDecimalStructRecord());
            BMap<BString, Object> map = (BMap<BString, Object>) object;
            assertEquals(map.get(fromString("value1")), new BigDecimal("2"));
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructTest8() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getStruct(),
                    Types.STRUCT, TestUtils.getStringStructRecord());
            BMap<BString, Object> map = (BMap<BString, Object>) object;
            assertEquals(map.get(fromString("value1")), "2");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertStructTest9() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            Object object = testClass.convertStruct(TestUtils.getRecordStruct(),
                    Types.STRUCT, TestUtils.getRecordStructRecord());
            BMap<BString, Object> map = (BMap<BString, Object>) object;
            MapValueImpl mapValue = (MapValueImpl) map.get(fromString("value0"));
            assertEquals(mapValue.getBooleanValue(fromString("value2")), true);
            assertEquals(mapValue.getBooleanValue(fromString("value1")), false);
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertDateTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.convertDate(null, Types.DATE,
                    PredefinedTypes.TYPE_STRING)
                    , "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertDateTest1() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        Date date = new Date();
        try {
            assertNotNull(testClass.convertDate(date, Types.DATE,
                    PredefinedTypes.TYPE_INT)
                    , "Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimeTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.convertTime(null, Types.TIME,
                    PredefinedTypes.TYPE_STRING)
                    , "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimeTest1() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        Date time = new Date();
        try {
            assertNotNull(testClass.convertTime(time, Types.TIME,
                    PredefinedTypes.TYPE_INT)
                    , "Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimeWithTimezoneTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.convertTimeWithTimezone(null, Types.TIME_WITH_TIMEZONE,
                    PredefinedTypes.TYPE_STRING)
                    , "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimeWithTimezoneTest1() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        OffsetTime offsetTime = OffsetTime.now();
        try {
            assertNotNull(testClass.convertTimeWithTimezone(offsetTime, Types.TIME_WITH_TIMEZONE,
                    PredefinedTypes.TYPE_INT)
                    , "Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimeStampTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.convertTimeStamp(null, Types.TIMESTAMP,
                    PredefinedTypes.TYPE_STRING)
                    , "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimeStampTest1() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        Date date = new Date();
        try {
            assertNotNull(testClass.convertTimeStamp(date, Types.TIMESTAMP,
                    PredefinedTypes.TYPE_INT)
                    , "Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimeStampTest2() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        Date date = new Date();
        try {
            assertNotNull(testClass.convertTimeStamp(date, Types.TIMESTAMP,
                    TestUtils.getTupleType())
                    , "Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimestampWithTimezoneTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.convertTimestampWithTimezone(null, Types.TIMESTAMP_WITH_TIMEZONE,
                    PredefinedTypes.TYPE_STRING)
                    , "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimestampWithTimezoneTest1() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        OffsetDateTime offsetDateTime = OffsetDateTime.now();
        try {
            assertNotNull(testClass.convertTimestampWithTimezone(offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE,
                    PredefinedTypes.TYPE_INT)
                    , "Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertTimestampWithTimezoneTest2() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        OffsetDateTime offsetDateTime = OffsetDateTime.now();
        try {
            assertNotNull(testClass.convertTimestampWithTimezone(offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE,
                    TestUtils.getTupleType())
                    , "Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void convertBinaryTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNotNull(testClass.convertBinary("String", Types.BINARY, PredefinedTypes.TYPE_STRING)
                    , "Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void createAndPopulateCustomValueArrayTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.testCreateAndPopulateCustomValueArray(), "Not Null received");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }

    @Test
    void createUserDefinedTypeSubtypeTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.testCreateUserDefinedTypeSubtype();
        } catch (ApplicationError e) {
            assertEquals(e.getMessage(), "Error while retrieving data for unsupported type int to " +
                    "create $$returnType$$ record.");
        }
    }

    @Test
    void convertCharTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertEquals(testClass.testConvertChar(), fromString("4"));
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }
}
