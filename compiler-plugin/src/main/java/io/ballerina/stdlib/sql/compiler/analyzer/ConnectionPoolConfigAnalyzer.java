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
import io.ballerina.compiler.api.symbols.UnionTypeSymbol;
import io.ballerina.compiler.api.symbols.VariableSymbol;
import io.ballerina.compiler.syntax.tree.BasicLiteralNode;
import io.ballerina.compiler.syntax.tree.ExpressionNode;
import io.ballerina.compiler.syntax.tree.MappingConstructorExpressionNode;
import io.ballerina.compiler.syntax.tree.MappingFieldNode;
import io.ballerina.compiler.syntax.tree.ModuleVariableDeclarationNode;
import io.ballerina.compiler.syntax.tree.Node;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.compiler.syntax.tree.SpecificFieldNode;
import io.ballerina.compiler.syntax.tree.UnaryExpressionNode;
import io.ballerina.compiler.syntax.tree.VariableDeclarationNode;
import io.ballerina.projects.plugins.AnalysisTask;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.stdlib.sql.compiler.Constants;
import io.ballerina.tools.diagnostics.Diagnostic;
import io.ballerina.tools.diagnostics.DiagnosticFactory;
import io.ballerina.tools.diagnostics.DiagnosticInfo;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;

import java.util.List;
import java.util.Optional;

import static io.ballerina.stdlib.sql.compiler.Constants.BALLERINA;
import static io.ballerina.stdlib.sql.compiler.Constants.CONNECTION_POOL;
import static io.ballerina.stdlib.sql.compiler.Constants.SQL;
import static io.ballerina.stdlib.sql.compiler.Constants.UNNECESSARY_CHARS_REGEX;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_101;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_102;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_103;

/**
 * ConnectionPoolConfigAnalyzer.
 */
public class ConnectionPoolConfigAnalyzer implements AnalysisTask<SyntaxNodeAnalysisContext> {

    @Override
    public void perform(SyntaxNodeAnalysisContext ctx) {
        List<Diagnostic> diagnostics = ctx.semanticModel().diagnostics();
        for (Diagnostic diagnostic : diagnostics) {
            if (diagnostic.diagnosticInfo().severity() == DiagnosticSeverity.ERROR) {
                return;
            }
        }
        Optional<Symbol> varSymOptional = ctx.semanticModel()
                .symbol(ctx.node());
        if (varSymOptional.isPresent()) {
            TypeSymbol typeSymbol = ((VariableSymbol) varSymOptional.get()).typeDescriptor();
            if (!isConnectionPoolVariable(typeSymbol)) {
                return;
            }

            // Initiated with a record
            Optional<ExpressionNode> optionalInitializer;
            if ((ctx.node() instanceof VariableDeclarationNode)) {
                // Function level variables
                optionalInitializer = ((VariableDeclarationNode) ctx.node()).initializer();
            } else {
                // Module level variables
                optionalInitializer = ((ModuleVariableDeclarationNode) ctx.node()).initializer();
            }
            if (optionalInitializer.isEmpty()) {
                return;
            }
            ExpressionNode initializer = optionalInitializer.get();
            if (!(initializer instanceof MappingConstructorExpressionNode)) {
                return;
            }

            SeparatedNodeList<MappingFieldNode> fields =
                    ((MappingConstructorExpressionNode) initializer).fields();
            for (MappingFieldNode field : fields) {
                String name = ((SpecificFieldNode) field).fieldName().toString()
                        .trim().replaceAll(UNNECESSARY_CHARS_REGEX, "");
                ExpressionNode valueNode = ((SpecificFieldNode) field).valueExpr().get();
                switch (name) {
                    case Constants.ConnectionPool.MAX_OPEN_CONNECTIONS:
                        int maxOpenConnections = Integer.parseInt(getTerminalNodeValue(valueNode));
                        if (maxOpenConnections < 1) {
                            DiagnosticInfo diagnosticInfo = new DiagnosticInfo(SQL_101.getCode(), SQL_101.getMessage(),
                                    SQL_101.getSeverity());

                            ctx.reportDiagnostic(
                                    DiagnosticFactory.createDiagnostic(diagnosticInfo, valueNode.location()));

                        }
                        break;
                    case Constants.ConnectionPool.MIN_IDLE_CONNECTIONS:
                        int minIdleConnection = Integer.parseInt(getTerminalNodeValue(valueNode));
                        if (minIdleConnection < 0) {
                            DiagnosticInfo diagnosticInfo = new DiagnosticInfo(SQL_102.getCode(), SQL_102.getMessage(),
                                    SQL_102.getSeverity());
                            ctx.reportDiagnostic(
                                    DiagnosticFactory.createDiagnostic(diagnosticInfo, valueNode.location()));

                        }
                        break;
                    case Constants.ConnectionPool.MAX_CONNECTION_LIFE_TIME:
                        float maxConnectionTime = Float.parseFloat(getTerminalNodeValue(valueNode));
                        if (maxConnectionTime < 30) {
                            DiagnosticInfo diagnosticInfo = new DiagnosticInfo(SQL_103.getCode(), SQL_103.getMessage(),
                                    SQL_103.getSeverity());
                            ctx.reportDiagnostic(
                                    DiagnosticFactory.createDiagnostic(diagnosticInfo, valueNode.location()));

                        }
                        break;
                    default:
                        // Can ignore all other fields
                        continue;
                }
            }
        }
    }

    private String getTerminalNodeValue(Node valueNode) {
        String value;
        if (valueNode instanceof BasicLiteralNode) {
            value = ((BasicLiteralNode) valueNode).literalToken().text();
        } else {
            UnaryExpressionNode unaryExpressionNode = (UnaryExpressionNode) valueNode;
            value = unaryExpressionNode.unaryOperator() +
                    ((BasicLiteralNode) unaryExpressionNode.expression()).literalToken().text();
        }
        return value.replaceAll(UNNECESSARY_CHARS_REGEX, "");
    }

    private boolean isConnectionPoolVariable(TypeSymbol type) {
        if (type.typeKind() == TypeDescKind.UNION) {
            return ((UnionTypeSymbol) type).memberTypeDescriptors().stream()
                    .filter(typeDescriptor -> typeDescriptor instanceof TypeReferenceTypeSymbol)
                    .map(typeReferenceTypeSymbol -> (TypeReferenceTypeSymbol) typeReferenceTypeSymbol)
                    .anyMatch(this::isSQLConnectionPoolVariable);
        }
        if (type.typeKind() == TypeDescKind.TYPE_REFERENCE) {
            return isSQLConnectionPoolVariable((TypeReferenceTypeSymbol) type);
        }
        return false;
    }

    private boolean isSQLConnectionPoolVariable(TypeReferenceTypeSymbol typeSymbol) {
        if (typeSymbol.typeDescriptor().typeKind() == TypeDescKind.RECORD) {
            ModuleSymbol moduleSymbol = typeSymbol.getModule().get();
            return SQL.equals(moduleSymbol.getName().get()) && BALLERINA.equals(moduleSymbol.id().orgName())
                    && typeSymbol.definition().getName().get().equals(CONNECTION_POOL);
        }
        return false;
    }
}
