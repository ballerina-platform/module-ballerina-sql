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

import io.ballerina.projects.DiagnosticResult;
import io.ballerina.projects.Package;
import io.ballerina.projects.PackageCompilation;
import io.ballerina.projects.ProjectEnvironmentBuilder;
import io.ballerina.projects.directory.BuildProject;
import io.ballerina.projects.environment.Environment;
import io.ballerina.projects.environment.EnvironmentBuilder;
import io.ballerina.tools.diagnostics.Diagnostic;
import io.ballerina.tools.diagnostics.DiagnosticInfo;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests the custom SQL compiler plugin.
 */
public class CompilerPluginTest {

    private static final Path RESOURCE_DIRECTORY = Paths.get("src", "test", "resources", "diagnostics")
            .toAbsolutePath();
    private static final Path DISTRIBUTION_PATH = Paths.get("../", "target", "ballerina-runtime")
            .toAbsolutePath();

    private static ProjectEnvironmentBuilder getEnvironmentBuilder() {
        Environment environment = EnvironmentBuilder.getBuilder().setBallerinaHome(DISTRIBUTION_PATH).build();
        return ProjectEnvironmentBuilder.getBuilder(environment);
    }

    private Package loadPackage(String path) {
        Path projectDirPath = RESOURCE_DIRECTORY.resolve(path);
        BuildProject project = BuildProject.load(getEnvironmentBuilder(), projectDirPath);
        return project.currentPackage();
    }

    @Test
    public void testInvalidConnectionParamConfig() {
        Package currentPackage = loadPackage("sample1");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        List<Diagnostic> errorDiagnosticsList = diagnosticResult.diagnostics().stream()
                .filter(r -> r.diagnosticInfo().severity().equals(DiagnosticSeverity.ERROR))
                .collect(Collectors.toList());
        long availableErrors = errorDiagnosticsList.size();

        Assert.assertEquals(availableErrors, 4);

        DiagnosticInfo maxOpenConnectionZero = errorDiagnosticsList.get(0).diagnosticInfo();
        Assert.assertEquals(maxOpenConnectionZero.code(), SQLDiagnosticsCodes.SQL_101.getCode());
        Assert.assertEquals(maxOpenConnectionZero.messageFormat(),
                "invalid value: expected value is greater than one");

        DiagnosticInfo maxConnectionLifeTime = errorDiagnosticsList.get(1).diagnosticInfo();
        Assert.assertEquals(maxConnectionLifeTime.code(), SQLDiagnosticsCodes.SQL_103.getCode());
        Assert.assertEquals(maxConnectionLifeTime.messageFormat(),
                "invalid value: expected value is greater than or equal to 30");

        DiagnosticInfo minIdleConnections = errorDiagnosticsList.get(2).diagnosticInfo();
        Assert.assertEquals(minIdleConnections.code(), SQLDiagnosticsCodes.SQL_102.getCode());
        Assert.assertEquals(minIdleConnections.messageFormat(),
                "invalid value: expected value is greater than zero");

        DiagnosticInfo maxOpenConnectionNegative = errorDiagnosticsList.get(3).diagnosticInfo();
        Assert.assertEquals(maxOpenConnectionNegative.code(), SQLDiagnosticsCodes.SQL_101.getCode());
        Assert.assertEquals(maxOpenConnectionNegative.messageFormat(),
                "invalid value: expected value is greater than one");
    }

    @Test
    public void testOutParameterValidations() {
        Package currentPackage = loadPackage("sample2");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        List<Diagnostic> errorDiagnosticsList = diagnosticResult.diagnostics().stream()
                .filter(r -> r.diagnosticInfo().severity().equals(DiagnosticSeverity.ERROR))
                .collect(Collectors.toList());
        long availableErrors = errorDiagnosticsList.size();

        Assert.assertEquals(availableErrors, 2);

        DiagnosticInfo charOutParameter = errorDiagnosticsList.get(0).diagnosticInfo();
        Assert.assertEquals(charOutParameter.code(), SQLDiagnosticsCodes.SQL_211.getCode());
        Assert.assertEquals(charOutParameter.messageFormat(),
                "invalid value: expected value is either string or json");

        DiagnosticInfo maxConnectionLifeTime = errorDiagnosticsList.get(1).diagnosticInfo();
        Assert.assertEquals(maxConnectionLifeTime.code(), SQLDiagnosticsCodes.SQL_223.getCode());
        Assert.assertEquals(maxConnectionLifeTime.messageFormat(),
                "invalid value: expected value is any one of time:TimeOfDay, int or string");
    }

}
