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

package org.apache.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.CascadeExtractionFn;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SimpleExtraction;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.http.SqlParameter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class CalciteTestBase
{
  public static final List<SqlParameter> DEFAULT_PARAMETERS = ImmutableList.of();

  /**
   * Injector for this test. This is the one used by the test cases. There is
   * a static one in CalciteTests: we no longer use that here.
   * <p>
   * The injector is common to the test <i>class</i>, and is shared by all
   * test <i>methods</i> (instances) within a test class. That is, the injector
   * is created before the first test, lives through all test methods, and is
   * nulled out after the last test. The idea is that, as in a server, the
   * injector holds global, static state which is not affected by an individual
   * test. See {@link QueryTester} for how to make per-test adjustments.
   *
   * @see #injector()
   * @see #injectorBuilder()
   * @see #buildInjector(DruidInjectorBuilder)
   */
  private static Injector injector;

  /**
   * Returns the injector for this test. Creates the injector lazily on demand
   * so that static methods on the test class can add Guice modules. When such
   * modules are provided, the injector is created at that time. Otherwise, it
   * is created on first use, using the "standard" set of modules that handle
   * "typical" SQL queries.
   */
  public Injector injector()
  {
    if (injector == null) {
      buildInjector(injectorBuilder().withSqlAggregation());
    }
    return injector;
  }

  /**
   * Create a Calcite injector builder with core modules enabled.
   * Use this form to perform customization beyond just adding
   * modules.
   *
   * @see {@link #buildInjector(DruidInjectorBuilder)}
   * to build the injector from the builder returned here
   */
  protected static CalciteTestInjectorBuilder injectorBuilder()
  {
    Preconditions.checkState(injector == null);
    CalciteTestInjectorBuilder builder = new CalciteTestInjectorBuilder()
        .withCalciteTestComponents();
    builder.add(
        new MockModules.CalciteQueryTestModule(),
        new MockModules.MockSqlIngestionModule()
    );
    return builder;
  }

  /**
   * Build the injector for the tests using the builder provided. This
   * builder typically is the one returned from {@link #injectorBuilder()},
   * but it can be any builder if the test has "special needs."
   */
  protected static void buildInjector(DruidInjectorBuilder builder)
  {
    Preconditions.checkState(injector == null);
    injector = builder.build();
  }

  /**
   * Tests can customize the injector by using a {@code @BeforeClass} annotation,
   * then calling {@link #setupInjector} with a list of additional modules.
   * Otherwise, the injector is created on first reference using default
   * modules.
   * <p>
   * Use this form when a test only needs to add modules, and <i>does not</i>
   * want the standard SQL aggregation module.
   *
   * @see {@link #injectorBuilder()} for how to take full
   * control of the injector-building process
   */
  protected static void setupInjector(com.google.inject.Module...modules)
  {
    buildInjector(injectorBuilder().addModules(modules));
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    injector = null;
  }

  @BeforeClass
  public static void setupCalciteProperties()
  {
    NullHandling.initializeForTests();
    ExpressionProcessing.initializeForTests(null);
  }

  public ObjectMapper queryJsonMapper()
  {
    return injector().getInstance(ObjectMapper.class);
  }

  protected ExprMacroTable exprMacroTable()
  {
    return injector().getInstance(ExprMacroTable.class);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeColumnExpression(final String column)
  {
    return DruidExpression.ofColumn(ColumnType.STRING, column);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeExpression(final String staticExpression)
  {
    return makeExpression(ColumnType.STRING, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeExpression(final ColumnType columnType, final String staticExpression)
  {
    return makeExpression(columnType, null, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeExpression(final SimpleExtraction simpleExtraction, final String staticExpression)
  {
    return makeExpression(ColumnType.STRING, simpleExtraction, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeExpression(
      final ColumnType columnType,
      final SimpleExtraction simpleExtraction,
      final String staticExpression
  )
  {
    return DruidExpression.ofExpression(
        columnType,
        simpleExtraction,
        (args) -> staticExpression,
        Collections.emptyList()
    );
  }

  // Generate timestamps for expected results
  public static long timestamp(final String timeString)
  {
    return Calcites.jodaToCalciteTimestamp(DateTimes.of(timeString), DateTimeZone.UTC);
  }

  // Generate timestamps for expected results
  public static long timestamp(final String timeString, final String timeZoneString)
  {
    final DateTimeZone timeZone = DateTimes.inferTzFromString(timeZoneString);
    return Calcites.jodaToCalciteTimestamp(new DateTime(timeString, timeZone), timeZone);
  }

  // Generate day numbers for expected results
  public static int day(final String dayString)
  {
    return (int) (Intervals.utc(timestamp("1970"), timestamp(dayString)).toDurationMillis() / (86400L * 1000L));
  }

  public static QuerySegmentSpec querySegmentSpec(final Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  public static AndDimFilter and(DimFilter... filters)
  {
    return new AndDimFilter(Arrays.asList(filters));
  }

  public static OrDimFilter or(DimFilter... filters)
  {
    return new OrDimFilter(Arrays.asList(filters));
  }

  public static NotDimFilter not(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  public static InDimFilter in(String dimension, List<String> values, ExtractionFn extractionFn)
  {
    return new InDimFilter(dimension, values, extractionFn);
  }

  public static SelectorDimFilter selector(final String fieldName, final String value, final ExtractionFn extractionFn)
  {
    return new SelectorDimFilter(fieldName, value, extractionFn);
  }

  public ExpressionDimFilter expressionFilter(final String expression)
  {
    return new ExpressionDimFilter(expression, exprMacroTable());
  }

  public static DimFilter numericSelector(
      final String fieldName,
      final String value,
      final ExtractionFn extractionFn
  )
  {
    // We use Bound filters for numeric equality to achieve "10.0" = "10"
    return bound(fieldName, value, value, false, false, extractionFn, StringComparators.NUMERIC);
  }

  public static BoundDimFilter bound(
      final String fieldName,
      final String lower,
      final String upper,
      final boolean lowerStrict,
      final boolean upperStrict,
      final ExtractionFn extractionFn,
      final StringComparator comparator
  )
  {
    return new BoundDimFilter(fieldName, lower, upper, lowerStrict, upperStrict, null, extractionFn, comparator);
  }

  public static BoundDimFilter timeBound(final Object intervalObj)
  {
    final Interval interval = new Interval(intervalObj, ISOChronology.getInstanceUTC());
    return new BoundDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        String.valueOf(interval.getStartMillis()),
        String.valueOf(interval.getEndMillis()),
        false,
        true,
        null,
        null,
        StringComparators.NUMERIC
    );
  }

  public static CascadeExtractionFn cascade(final ExtractionFn... fns)
  {
    return new CascadeExtractionFn(fns);
  }

  public static List<DimensionSpec> dimensions(final DimensionSpec... dimensionSpecs)
  {
    return Arrays.asList(dimensionSpecs);
  }

  public static List<AggregatorFactory> aggregators(final AggregatorFactory... aggregators)
  {
    return Arrays.asList(aggregators);
  }

  public static DimFilterHavingSpec having(final DimFilter filter)
  {
    return new DimFilterHavingSpec(filter, true);
  }

  public ExpressionVirtualColumn expressionVirtualColumn(
      final String name,
      final String expression,
      final ColumnType outputType
  )
  {
    return new ExpressionVirtualColumn(name, expression, outputType, exprMacroTable());
  }

  public JoinDataSource join(
      DataSource left,
      DataSource right,
      String rightPrefix,
      String condition,
      JoinType joinType,
      DimFilter filter
  )
  {
    return JoinDataSource.create(
        left,
        right,
        rightPrefix,
        condition,
        joinType,
        filter,
        exprMacroTable()
    );
  }

  public JoinDataSource join(
      DataSource left,
      DataSource right,
      String rightPrefix,
      String condition,
      JoinType joinType
  )
  {
    return join(left, right, rightPrefix, condition, joinType, null);
  }

  public static String equalsCondition(DruidExpression left, DruidExpression right)
  {
    return StringUtils.format("(%s == %s)", left.getExpression(), right.getExpression());
  }

  public ExpressionPostAggregator expressionPostAgg(final String name, final String expression)
  {
    return new ExpressionPostAggregator(name, expression, null, exprMacroTable());
  }

  public static Druids.ScanQueryBuilder newScanQueryBuilder()
  {
    return new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .legacy(false);
  }
}
