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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;

public class CatalogIngestionTest extends CalciteIngestionDmlTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    builder.addModule(new InputSourceModule());
  }

  @Test
  public void testReference()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("insertFromExternal")
        .verify();
  }

  protected static URI toURI(String uri)
  {
    try {
      return new URI(uri);
    }
    catch (URISyntaxException e) {
      throw new ISE("Bad URI: %s", uri);
    }
  }

  protected final ExternalDataSource httpDataSource = new ExternalDataSource(
      new HttpInputSource(
          Collections.singletonList(toURI("http:foo.com/bar.csv")),
          "bob",
          new DefaultPasswordProvider("secret"),
          new HttpInputSourceConfig(null)
      ),
      new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0),
      RowSignature.builder()
                  .add("x", ColumnType.STRING)
                  .add("y", ColumnType.STRING)
                  .add("z", ColumnType.LONG)
                  .build()
  );

  /**
   * Basic use of EXTERN
   */
  @Test
  public void testHttpExtern()
  {
    assertNotNull(queryFramework().injector().getInstance(HttpInputSourceConfig.class));
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM %s PARTITIONED BY ALL TIME", externSql(httpDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  protected String externSqlByName(final ExternalDataSource externalDataSource)
  {
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      return StringUtils.format(
          "TABLE(extern(inputSource => %s,\n" +
          "             inputFormat => %s,\n" +
          "             signature => %s))",
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputSource())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputFormat())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getSignature()))
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * EXTERN with parameters by name. Logical plan and native query are identical
   * to the basic EXTERN.
   */
  @Test
  public void testHttpExternByName()
  {
    assertNotNull(queryFramework().injector().getInstance(HttpInputSourceConfig.class));
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\nFROM %s\nPARTITIONED BY ALL TIME", externSqlByName(httpDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }

  /**
   * HTTP with parameters by name. Logical plan and native query are identical
   * to the basic EXTERN.
   */
  @Test
  public void testHttpFn()
  {
    assertNotNull(queryFramework().injector().getInstance(HttpInputSourceConfig.class));
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(http(\"user\" => 'bob', password => 'secret',\n" +
             "                uris => 'http:foo.com/bar.csv', format => 'csv'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", httpDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(httpDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteInsertDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("httpExtern")
        .verify();
  }
}
