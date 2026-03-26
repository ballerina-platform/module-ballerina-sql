/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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

/**
 * Parsed JDBC URL components for metric tagging. All fields are non-null
 * Strings — empty string means absent. Reconstructed {@code safeUrl}
 * is built from parsed components only (no credentials, no query params).
 *
 * @param host    database host (e.g., "localhost")
 * @param port    database port as string, or "" if absent
 * @param dbName  database name from URL path, or "" if absent
 * @param safeUrl reconstructed URL without credentials or query params
 * @since 1.18.0
 */
record JdbcUrlInfo(String host, String port, String dbName, String safeUrl) {

    static final JdbcUrlInfo EMPTY = new JdbcUrlInfo("", "", "", "");

    /**
     * Check whether URL parsing produced usable components.
     * Host is the canonical indicator — if host is empty, all other
     * fields are also empty.
     *
     * @return true if no URL components were extracted
     */
    boolean isEmpty() {
        return host.isEmpty();
    }
}
