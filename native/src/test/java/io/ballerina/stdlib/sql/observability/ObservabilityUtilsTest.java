/*
 * Copyright (c) 2025, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.sql.observability;

import com.zaxxer.hikari.metrics.PoolStats;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link ObservabilityUtils}.
 *
 * @since 1.18.0
 */
public class ObservabilityUtilsTest {

    // ---- Pool name generation tests ----

    @Test
    void testGeneratePoolNameUserConfigured() {
        String result = ObservabilityUtils.generatePoolName("MyPool");
        assertEquals(result, "MyPool");
    }

    @Test
    void testGeneratePoolNameNullInput() {
        String result = ObservabilityUtils.generatePoolName(null);
        assertValidUuid(result);
    }

    @Test
    void testGeneratePoolNameEmptyInput() {
        String result = ObservabilityUtils.generatePoolName("");
        assertValidUuid(result);
    }

    @Test
    void testGeneratePoolNameUniqueness() {
        String first = ObservabilityUtils.generatePoolName(null);
        String second = ObservabilityUtils.generatePoolName(null);
        assertNotEquals(first, second,
                "Two calls with no user name must produce different UUIDs");
    }

    @Test
    void testGeneratePoolNameWhitespaceOnly() {
        String result = ObservabilityUtils.generatePoolName("   ");
        assertValidUuid(result);
    }

    @Test
    void testGeneratePoolNameWithColons() {
        String result = ObservabilityUtils.generatePoolName("my:pool:name");
        assertEquals(result, "my-pool-name");
    }

    @Test
    void testGeneratePoolNameSpecialCharacters() {
        String result = ObservabilityUtils.generatePoolName("my pool/test;drop");
        assertEquals(result, "my-pool-test-drop");
    }

    @Test
    void testGeneratePoolNameUuidFormat() {
        String result = ObservabilityUtils.generatePoolName(null);
        assertFalse(result.contains(":"),
                "UUID must not contain colons");
        assertEquals(result.length(), 36,
                "UUID must be 36 characters (8-4-4-4-12)");
    }

    @Test
    void testGeneratePoolNameAllColons() {
        // ":::" → sanitized to "---" → collapsed to "-" → stripped → ""
        // → falls through to UUID
        String result = ObservabilityUtils.generatePoolName(":::");
        assertValidUuid(result);
    }

    @Test
    void testGeneratePoolNameDotsAndUnderscores() {
        String result = ObservabilityUtils.generatePoolName("my_pool.v2");
        assertEquals(result, "my_pool.v2");
    }

    @Test
    void testGeneratePoolNameMixedValidInvalid() {
        String result = ObservabilityUtils.generatePoolName("pool@host:5432/db");
        assertEquals(result, "pool-host-5432-db");
    }

    // ---- Safety guard tests ----

    @Test
    void testUnregisterEmptyPoolNameGuard() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "survivor-pool-1");
        ObservabilityUtils.unregisterPoolMetrics("");
        assertTrue(ObservabilityUtils.hasRegisteredMetrics("survivor-pool-1"),
                "Empty poolName unregister must not affect other pools");
        ObservabilityUtils.unregisterPoolMetrics("survivor-pool-1");
    }

    @Test
    void testUnregisterNullPoolNameGuard() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "survivor-pool-2");
        ObservabilityUtils.unregisterPoolMetrics(null);
        assertTrue(ObservabilityUtils.hasRegisteredMetrics("survivor-pool-2"),
                "Null poolName unregister must not affect other pools");
        ObservabilityUtils.unregisterPoolMetrics("survivor-pool-2");
    }

    @Test
    void testUnregisterDoubleCallIdempotent() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "double-unreg-pool");
        ObservabilityUtils.unregisterPoolMetrics("double-unreg-pool");
        ObservabilityUtils.unregisterPoolMetrics("double-unreg-pool");
    }

    // ---- Pool state refresh tests ----

    @Test
    void testRegisterAndRefreshPoolMetrics() {
        PoolStats stats = createMockPoolStats(5, 3, 8, 2, 10, 1);
        ObservabilityUtils.registerPoolMetrics(stats, "test-register-pool");
        ObservabilityUtils.unregisterPoolMetrics("test-register-pool");
    }

    @Test
    void testRefreshUnregisteredPool() {
        ObservabilityUtils.refreshPoolState("nonexistent");
    }

    @Test
    void testUnregisterIdempotent() {
        ObservabilityUtils.unregisterPoolMetrics("never-registered");
        ObservabilityUtils.unregisterPoolMetrics("never-registered");
    }

    // ---- Helpers ----

    private static void assertValidUuid(String value) {
        try {
            UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new AssertionError("Expected valid UUID but got: " + value, e);
        }
    }

    private static PoolStats createMockPoolStats(int active, int idle,
                                                  int total, int pending,
                                                  int max, int min) {
        return new PoolStats(1000L) {
            {
                this.activeConnections = active;
                this.idleConnections = idle;
                this.totalConnections = total;
                this.pendingThreads = pending;
                this.maxConnections = max;
                this.minConnections = min;
            }

            @Override
            protected void update() {
            }
        };
    }
}
