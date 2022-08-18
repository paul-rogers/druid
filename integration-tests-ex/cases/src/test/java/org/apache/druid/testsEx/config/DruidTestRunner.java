/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testsEx.config;

import junit.framework.Test;
import junitparams.internal.ParameterisedTestClassRunner;
import junitparams.internal.TestMethod;
import org.apache.druid.java.util.common.UOE;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.TestWithParameters;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Test runner for Druid integration tests. Loads test configuration. Initializes
 * Guice. Injects dependencies into the test. Runs the tests.
 * Shuts down afterwards. Since cluster configuration and health checks are a bit
 * expensive, does that work once per test class rather than once per test method.
 * <p>
 * Note that our Guice usage is a bit awkward for tests. Druid modules define
 * objects that must be lifecycle managed, but as lazy singletons, which means
 * that they might be created after the lifecycle starts, which causes the dreaded
 * "it doesn't work that way" message. The awkward workaround is to ask to inject
 * test members <i>before</i> starting the lifecycle, so that the injection creates
 * a reference, which creates the object, which registers it in the lifecycle. We
 * should fix this issue. Until then, the awkwardness is hidden in this test runner.
 */
public class DruidTestRunner extends BlockJUnit4ClassRunnerWithParameters
{
  public DruidTestRunner(TestWithParameters test) throws InitializationError
  {
    super(test);
  }

//  // Trying to merge DruidTestRunner with JUnitParamsRunner
//  /// Start Here///
//  private ParameterisedTestClassRunner parameterisedRunner = new ParameterisedTestClassRunner(super.getTestClass());
//
//  private boolean handleIgnored(FrameworkMethod method, RunNotifier notifier) {
//    TestMethod testMethod = this.parameterisedRunner.testMethodFor(method);
//    if (testMethod.isIgnored()) {
//      notifier.fireTestIgnored(this.describeMethod(method));
//    }
//
//    return testMethod.isIgnored();
//  }
//
//  private void verifyMethodCanBeRunByStandardRunner(TestMethod testMethod) {
//    List<Throwable> errors = new ArrayList();
//    testMethod.frameworkMethod().validatePublicVoidNoArg(false, errors);
//    if (!errors.isEmpty()) {
//      throw new RuntimeException((Throwable)errors.get(0));
//    }
//  }
//  @Override
//  protected void runChild(FrameworkMethod method, RunNotifier notifier) {
//    if (!this.handleIgnored(method, notifier)) {
//      TestMethod testMethod = this.parameterisedRunner.testMethodFor(method);
//      if (this.parameterisedRunner.shouldRun(testMethod)) {
//        this.parameterisedRunner.runParameterisedTest(testMethod, this.methodBlock(method), notifier);
//      } else {
//        this.verifyMethodCanBeRunByStandardRunner(testMethod);
//        super.runChild(method, notifier);
//      }
//
//    }
//  }
//
//  @Override
//  public Description getDescription() {
//    Description description = Description.createSuiteDescription(getName(), getTestClass().getAnnotations());
//    List<FrameworkMethod> resultMethods = parameterisedRunner.returnListOfMethods();
//
//    for (FrameworkMethod method : resultMethods)
//      description.addChild(describeMethod(method));
//
//    return description;
//  }
//
//  private Description describeMethod(FrameworkMethod method) {
//    Description child = parameterisedRunner.describeParameterisedMethod(method);
//
//    if (child == null)
//      child = describeChild(method);
//
//    return child;
//
//  }
//
//  @Override
//  protected Statement methodInvoker(FrameworkMethod method, Object test) {
//    Statement methodInvoker = parameterisedRunner.parameterisedMethodInvoker(method, test);
//    if (methodInvoker == null) {
//      methodInvoker = super.methodInvoker(method, test);
//    }
//
//    return methodInvoker;
//
//  }
//
//  /// End Here ///

  private class CloseInitializer extends Statement
  {
    private final Statement next;

    public CloseInitializer(Statement next)
    {
      this.next = next;
    }

    @Override
    public void evaluate() throws Throwable
    {
      next.evaluate();
      if (initializer != null) {
        initializer.close();
        initializer = null;
      }
    }
  }

  private Initializer initializer;

//  public DruidTestRunner(Class<?> testClass) throws InitializationError
//  {
//    super(testClass);
//  }

  @Override
  public Object createTest() throws Exception
  {
    Object test = super.createTest();
    if (initializer == null) {
      initializer = buildInitializer(test);
    } else {
      initializer.injector().injectMembers(test);
    }
    return test;
  }

  private Initializer buildInitializer(Object test)
  {
    Class<?> testClass = test.getClass();
    Category[] annotations = testClass.getAnnotationsByType(Category.class);
    if (annotations.length == 0) {
      throw new UOE(
          "Class % must have a @Category annotation",
          testClass.getSimpleName()
      );
    }
    if (annotations.length != 1) {
      throw new UOE(
          "Class % must have exactly one @Category annotation",
          testClass.getSimpleName()
      );
    }
    Class<?>[] categories = annotations[0].value();
    if (categories.length == 0) {
      throw new UOE(
          "Class % must have a @Category value",
          testClass.getSimpleName()
      );
    }
    if (categories.length != 1) {
      throw new UOE(
          "Class % must have exactly one @Category value",
          testClass.getSimpleName()
      );
    }
    Class<?> category = category(testClass);
    String clusterName = inferCluster(category);
    Initializer.Builder builder = Initializer.builder(clusterName)
        .test(test)
        .validateCluster();
    for (Method method : testClass.getMethods()) {
      if (method.getAnnotation(Configure.class) == null) {
        continue;
      }
      final int requiredMods = Modifier.STATIC | Modifier.PUBLIC;
      if ((method.getModifiers() & requiredMods) != requiredMods) {
        throw new UOE(
            "Method %s annotated with @Configure must be public static",
            method.getName()
          );
      }
      try {
        method.invoke(null, builder);
      }
      catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new UOE(
            "Call to Method %s annotated with @Configure failed: %s",
            method.getName(),
            e.getMessage()
        );
      }
    }
    return builder.build();
  }

  /**
   * Resolve the {@code @Category} annotation for the test class.
   */
  private Class<?> category(Class<?> testClass)
  {
    Category[] annotations = testClass.getAnnotationsByType(Category.class);
    if (annotations.length == 0) {
      throw new UOE(
          "Class % must have a @Category annotation",
          testClass.getSimpleName()
      );
    }
    if (annotations.length != 1) {
      throw new UOE(
          "Class % must have exactly one @Category annotation",
          testClass.getSimpleName()
      );
    }
    Class<?>[] categories = annotations[0].value();
    if (categories.length == 0) {
      throw new UOE(
          "Class % must have a @Category value",
          testClass.getSimpleName()
      );
    }
    if (categories.length != 1) {
      throw new UOE(
          "Class % must have exactly one @Category value",
          testClass.getSimpleName()
      );
    }
    return categories[0];
  }

  /**
   * Resolve the optional {@code @Cluster} annotation on the test category.
   * If omitted, the category itself is the cluster name.
   */
  private String inferCluster(Class<?> category)
  {
    String categoryName = category.getSimpleName();
    Cluster[] annotations = category.getAnnotationsByType(Cluster.class);
    if (annotations.length == 0) {
      return categoryName;
    }
    if (annotations.length != 1) {
      throw new UOE(
          "Category % must have no more than one @Cluster annotation",
          category.getSimpleName()
      );
    }
    Class<?> clusterClass = annotations[0].value();
    return clusterClass.getSimpleName();
  }

  @Override
  protected Statement withAfterClasses(Statement statement)
  {
    return new CloseInitializer(super.withAfterClasses(statement));
  }
}
