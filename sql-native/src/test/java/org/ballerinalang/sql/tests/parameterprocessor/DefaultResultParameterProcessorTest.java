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

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.sql.parameterprocessor.DefaultResultParameterProcessor;
import org.ballerinalang.sql.utils.ColumnDefinition;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    }

    @Test
    void createAndPopulateCustomBBRefValueArrayTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.testNullCreateAndPopulateCustomBBRefValueArray(), "Not Null received");
        } catch (Exception ignored) {

        }
    }

    @Test
    void createUserDefinedTypeTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.testNullCreateUserDefinedType(), "Not Null received");
        } catch (Exception ignored) {

        }
    }

    @Test
    void getCustomResultTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        ColumnDefinition columnDefinition = new ColumnDefinition("int_type", null,
                2, "INT", TypeUtils.getType(1), false);
        try {
            testClass.getCustomResult(null, 1, columnDefinition);
        } catch (ApplicationError e) {
            assertEquals(e.getMessage(), "Unsupported SQL type INT");
        }
    }

    @Test
    void populateCustomOutParametersTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            testClass.populateCustomOutParameters(null, null, 2, 3);
        } catch (ApplicationError e) {
            assertEquals(e.getMessage(), "Unsupported SQL type '3' when reading Procedure call Out" +
                    " parameter of index '2'.");
        }
    }

    @Test
    void getCustomOutParametersTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        ColumnDefinition columnDefinition = new ColumnDefinition("int_type", null,
                2, "INT", TypeUtils.getType(1), false);
        try {
            Object object = testClass.getCustomOutParameters(null, 1, null);
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

        }
    }

    @Test
    void convertStructTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNull(testClass.convertStruct(null, Types.STRUCT, PredefinedTypes.STRING_ITR_NEXT_RETURN_TYPE)
                    , "Not Null received");
        } catch (Exception ignored) {

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

        }
    }

    @Test
    void convertTimeStampTest2() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        Date date = new Date();
        try {
            assertNotNull(testClass.convertTimeStamp(date, Types.TIMESTAMP,
                    PredefinedTypes.TYPE_READONLY_PROCESSING_INSTRUCTION)
                    , "Null received");
        } catch (Exception ignored) {

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

        }
    }

    @Test
    void convertTimestampWithTimezoneTest2() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        OffsetDateTime offsetDateTime = OffsetDateTime.now();
        try {
            assertNotNull(testClass.convertTimestampWithTimezone(offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE,
                    PredefinedTypes.TYPE_READONLY_PROCESSING_INSTRUCTION)
                    , "Null received");
        } catch (Exception ignored) {

        }
    }

    @Test
    void convertBinaryTest() {
        NullAndErrorCheckClass testClass = new NullAndErrorCheckClass();
        try {
            assertNotNull(testClass.convertBinary("String", Types.BINARY, PredefinedTypes.TYPE_STRING)
                    , "Null received");
        } catch (Exception ignored) {

        }
    }
}
