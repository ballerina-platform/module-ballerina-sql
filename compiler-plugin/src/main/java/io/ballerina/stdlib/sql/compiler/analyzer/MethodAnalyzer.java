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

package io.ballerina.stdlib.sql.compiler.analyzer;

import io.ballerina.compiler.api.symbols.ModuleSymbol;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.api.symbols.TypeDescKind;
import io.ballerina.compiler.api.symbols.TypeReferenceTypeSymbol;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.syntax.tree.ExpressionNode;
import io.ballerina.compiler.syntax.tree.FunctionArgumentNode;
import io.ballerina.compiler.syntax.tree.MethodCallExpressionNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.projects.plugins.AnalysisTask;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.stdlib.sql.compiler.Constants;
import io.ballerina.stdlib.sql.compiler.Utils;
import io.ballerina.tools.diagnostics.Diagnostic;
import io.ballerina.tools.diagnostics.DiagnosticFactory;
import io.ballerina.tools.diagnostics.DiagnosticInfo;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;

import java.util.List;
import java.util.Optional;

/**
 * Code Analyser for OutParameter get method type validations.
 */
public class MethodAnalyzer implements AnalysisTask<SyntaxNodeAnalysisContext> {
    @Override
    public void perform(SyntaxNodeAnalysisContext ctx) {
        List<Diagnostic> diagnostics = ctx.semanticModel().diagnostics();
        for (Diagnostic diagnostic : diagnostics) {
            if (diagnostic.diagnosticInfo().severity() == DiagnosticSeverity.ERROR) {
                return;
            }
        }

        MethodCallExpressionNode methodCallExpNode = (MethodCallExpressionNode) ctx.node();
        // Get the object type to validate arguments
        ExpressionNode methodExpression = methodCallExpNode.expression();
        Optional<TypeSymbol> methodExpReferenceType = ctx.semanticModel().typeOf(methodExpression);
        if (methodExpReferenceType.isEmpty()) {
            return;
        }
        if (methodExpReferenceType.get().typeKind() != TypeDescKind.TYPE_REFERENCE) {
            return;
        }
        TypeReferenceTypeSymbol methodExpTypeSymbol = (TypeReferenceTypeSymbol) methodExpReferenceType.get();
        Optional<ModuleSymbol> optionalModuleSymbol = methodExpTypeSymbol.getModule();
        if (optionalModuleSymbol.isEmpty()) {
            return;
        }
        ModuleSymbol module = optionalModuleSymbol.get();
        if (!(module.id().orgName().equals(Constants.BALLERINA) && module.id().moduleName().equals(Constants.SQL))) {
            return;
        }
        String objectName = ((TypeReferenceTypeSymbol) methodExpReferenceType.get()).definition().getName().get();

        // Filter by method name, only OutParameter objects have get method
        Optional<Symbol> methodSymbol = ctx.semanticModel().symbol(methodCallExpNode.methodName());
        if (methodSymbol.isEmpty()) {
            return;
        }
        Optional<String> methodName = methodSymbol.get().getName();
        if (methodName.isEmpty()) {
            return;
        }
        if (!methodName.get().equals(Constants.OutParameter.METHOD_NAME)) {
            return;
        }

        // Filter by parameters length
        SeparatedNodeList<FunctionArgumentNode> arguments = methodCallExpNode.arguments();
        if (arguments.size() != 1) {
            return;
        }
        TypeSymbol argumentTypeSymbol =
                ((TypeSymbol) ctx.semanticModel().symbol(methodCallExpNode.arguments().get(0)).get());
        TypeDescKind argTypeKind = argumentTypeSymbol.typeKind();
        DiagnosticInfo diagnosticsForInvalidTypes = Utils.addDiagnosticsForInvalidTypes(objectName, argTypeKind);
        if (diagnosticsForInvalidTypes != null) {
            ctx.reportDiagnostic(DiagnosticFactory.createDiagnostic(diagnosticsForInvalidTypes,
                    methodCallExpNode.arguments().get(0).location()));
        }
    }
}
