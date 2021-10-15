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

package io.ballerina.stdlib.sql.exception;

import org.testng.annotations.Test;

import java.sql.SQLException;

import static org.testng.Assert.assertEquals;

/**
 * ApplicationError class test.
 *
 * @since 0.6.0-beta.2
 */
public class ApplicationErrorTest {
    @Test
    void applicationErrorTest1() {
        ApplicationError error = new ApplicationError("Application Error", new SQLException("new SQL Exception"));
        assertEquals(error.getMessage(), "Application Error");
    }

    @Test
    void conversionErrorTest1() {
        ConversionError error = new ConversionError("Conversion Error", new SQLException("new SQL Exception"));
        assertEquals(error.getMessage(), "Conversion Error");
    }

    @Test
    void conversionErrorTest2() {
        ConversionError error = new ConversionError("Conversion Error");
        assertEquals(error.getMessage(), "Conversion Error");
    }

    @Test
    void conversionErrorTest3() {
        ConversionError error = new ConversionError("{}sdf", "JSON", "Expected :");
        assertEquals(error.getMessage(), "Retrieved result '{}sdf' could not be converted to 'JSON', Expected :.");
    }

    @Test
    void fieldMismatchErrorTest1() {
        FieldMismatchError error = new FieldMismatchError("FieldMismatch Error", new SQLException("new SQL Exception"));
        assertEquals(error.getMessage(), "FieldMismatch Error");
    }

    @Test
    void fieldMismatchErrorTest2() {
        FieldMismatchError error = new FieldMismatchError("FieldMismatch Error");
        assertEquals(error.getMessage(), "FieldMismatch Error");
    }

    @Test
    void unsupportedTypeErrorTest2() {
        UnsupportedTypeError error = new UnsupportedTypeError("Unsupported Error",
                new SQLException("new SQL Exception"));
        assertEquals(error.getMessage(), "Unsupported Error");
    }

    @Test
    void typeMismatchErrorTest1() {
        TypeMismatchError error = new TypeMismatchError("TypeMismatch Error", new SQLException("new SQL Exception"));
        assertEquals(error.getMessage(), "TypeMismatch Error");
    }

    @Test
    void typeMismatchErrorTest2() {
        TypeMismatchError error = new TypeMismatchError("SQL Time", "byte",
                new String[]{"time:TimeOfDay", "time:Time"});
        assertEquals(error.getMessage(),
                "The ballerina type expected for 'SQL Time' type are 'time:TimeOfDay', and 'time:Time' " +
                        "but found type 'byte'.");
    }

}
