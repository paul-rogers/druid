package org.apache.druid.testsEx.config;

import org.junit.runner.Runner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;


public class DruidTestRunnerFactory implements ParametersRunnerFactory {
  @Override
  public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
    return new DruidTestRunner(test);
  }
}
