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

package io.ballerina.stdlib.sql.tests.exception;

import io.ballerina.stdlib.sql.exception.ApplicationError;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
