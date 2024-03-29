/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.sql.compiler;

import io.ballerina.compiler.syntax.tree.SyntaxKind;
import io.ballerina.projects.plugins.CodeAnalysisContext;
import io.ballerina.projects.plugins.CodeAnalyzer;
import io.ballerina.stdlib.sql.compiler.analyzer.ConnectionPoolConfigAnalyzer;
import io.ballerina.stdlib.sql.compiler.analyzer.MethodAnalyzer;

import java.util.List;

/**
 * SQL Code Analyzer.
 */
public class SQLCodeAnalyzer extends CodeAnalyzer {

    @Override
    public void init(CodeAnalysisContext codeAnalysisContext) {
        codeAnalysisContext.addSyntaxNodeAnalysisTask(new ConnectionPoolConfigAnalyzer(),
                List.of(SyntaxKind.LOCAL_VAR_DECL, SyntaxKind.MODULE_VAR_DECL));
        codeAnalysisContext.addSyntaxNodeAnalysisTask(new MethodAnalyzer(), SyntaxKind.METHOD_CALL);
    }
}
