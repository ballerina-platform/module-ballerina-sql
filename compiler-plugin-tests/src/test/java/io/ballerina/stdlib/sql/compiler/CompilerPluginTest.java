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

        Assert.assertEquals(availableErrors, 6);

        for (int i = 0; i < errorDiagnosticsList.size(); i++) {
            switch (i) {
                case 0:
                case 2:
                case 5:
                    Assert.assertEquals(errorDiagnosticsList.get(i).diagnosticInfo().code(),
                            SQLDiagnosticsCodes.SQL_103.getCode());
                    Assert.assertEquals(errorDiagnosticsList.get(i).diagnosticInfo().messageFormat(),
                            SQLDiagnosticsCodes.SQL_103.getMessage());
                    break;
                case 1:
                case 4:
                    Assert.assertEquals(errorDiagnosticsList.get(i).diagnosticInfo().code(),
                            SQLDiagnosticsCodes.SQL_101.getCode());
                    Assert.assertEquals(errorDiagnosticsList.get(i).diagnosticInfo().messageFormat(),
                            SQLDiagnosticsCodes.SQL_101.getMessage());
                    break;

                case 3:
                    Assert.assertEquals(errorDiagnosticsList.get(i).diagnosticInfo().code(),
                            SQLDiagnosticsCodes.SQL_102.getCode());
                    Assert.assertEquals(errorDiagnosticsList.get(i).diagnosticInfo().messageFormat(),
                            SQLDiagnosticsCodes.SQL_102.getMessage());
                    break;
                default:
                    Assert.fail();
            }
        }
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

        Assert.assertEquals(availableErrors, 28);

        DiagnosticInfo charOutParameter = errorDiagnosticsList.get(0).diagnosticInfo();
        Assert.assertEquals(charOutParameter.code(), SQLDiagnosticsCodes.SQL_211.getCode());
        Assert.assertEquals(charOutParameter.messageFormat(), SQLDiagnosticsCodes.SQL_211.getMessage());

        DiagnosticInfo timeOutParameter = errorDiagnosticsList.get(1).diagnosticInfo();
        Assert.assertEquals(timeOutParameter.code(), SQLDiagnosticsCodes.SQL_223.getCode());
        Assert.assertEquals(timeOutParameter.messageFormat(), SQLDiagnosticsCodes.SQL_223.getMessage());

        DiagnosticInfo dateOutParameter = errorDiagnosticsList.get(2).diagnosticInfo();
        Assert.assertEquals(dateOutParameter.code(), SQLDiagnosticsCodes.SQL_222.getCode());
        Assert.assertEquals(dateOutParameter.messageFormat(), SQLDiagnosticsCodes.SQL_222.getMessage());

        DiagnosticInfo timeWithTimezoneOutParameter = errorDiagnosticsList.get(3).diagnosticInfo();
        Assert.assertEquals(timeWithTimezoneOutParameter.code(), SQLDiagnosticsCodes.SQL_231.getCode());
        Assert.assertEquals(timeWithTimezoneOutParameter.messageFormat(), SQLDiagnosticsCodes.SQL_231.getMessage());
    }

    @Test
    public void testConnectionPoolWithVariable() {
        Package currentPackage = loadPackage("sample4");
        PackageCompilation compilation = currentPackage.getCompilation();
        DiagnosticResult diagnosticResult = compilation.diagnosticResult();
        List<Diagnostic> errorDiagnosticsList = diagnosticResult.diagnostics().stream()
                .filter(r -> r.diagnosticInfo().severity().equals(DiagnosticSeverity.ERROR))
                .collect(Collectors.toList());
        long availableErrors = errorDiagnosticsList.size();

        Assert.assertEquals(availableErrors, 0);
    }
}
