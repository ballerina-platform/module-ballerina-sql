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

package io.ballerina.stdlib.sql.transaction;

import io.ballerina.stdlib.sql.TestUtils;
import org.testng.annotations.Test;

import java.sql.Connection;

import javax.transaction.xa.XAResource;

import static org.testng.Assert.assertEquals;

/**
 * SQLTransactionContext class test.
 *
 * @since 0.6.0-beta.2
 */
public class SQLTransactionContextTest {
    XAResource xaResource = TestUtils.getMockXAResource();

    @Test
    void commitTest1() {
        Connection connection = TestUtils.getMockConnection(false);
        SQLTransactionContext sqlTransactionContext = new SQLTransactionContext(connection, xaResource);
        try {
            sqlTransactionContext.commit();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "transaction commit failed:Commit Error");
        }
    }

    @Test
    void rollbackTest() {
        Connection connection = TestUtils.getMockConnection(false);
        SQLTransactionContext sqlTransactionContext = new SQLTransactionContext(connection, xaResource);
        try {
            sqlTransactionContext.rollback();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "transaction rollback failed:Rollback Error");
        }
    }

    @Test
    void rollbackTest1() {
        Connection connection = TestUtils.getMockConnection(true);
        SQLTransactionContext sqlTransactionContext = new SQLTransactionContext(connection, xaResource);
        try {
            sqlTransactionContext.rollback();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "transaction rollback failed:Rollback Error");
        }
    }

    @Test
    void closeTest() {
        Connection connection = TestUtils.getMockConnection(false);
        SQLTransactionContext sqlTransactionContext = new SQLTransactionContext(connection, xaResource);
        try {
            sqlTransactionContext.close();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "connection close failed:Close Error");
        }
    }

    @Test
    void closeTest1() {
        Connection connection = TestUtils.getMockConnection(true);
        SQLTransactionContext sqlTransactionContext = new SQLTransactionContext(connection, xaResource);
        try {
            sqlTransactionContext.close();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "connection close failed:Close Error");
        }
    }
}
