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
package io.ballerina.stdlib.sql;

/**
 * Processed Details from sql:ParameterizedQuery.
 */
public class ParameterizedQuery {
    private final String sqlQuery;
    private final Object[] insertions;

    public ParameterizedQuery(String sqlQuery, Object[] insertions) {
        this.sqlQuery = sqlQuery;
        this.insertions = insertions;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public Object[] getInsertions() {
        return insertions;
    }
}
