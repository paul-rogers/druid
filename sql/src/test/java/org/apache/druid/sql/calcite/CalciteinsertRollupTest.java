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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.junit.Test;

import java.util.Map;

public class CalciteinsertRollupTest extends CalciteIngestionDmlTest
{
  // Here because the one in MSQ is not visible.
  public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";

  public static final Map<String, Object> WITH_ROLLUP_CONTEXT = ImmutableMap.<String, Object>builder()
      .putAll(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
      .put(CTX_FINALIZE_AGGREGATIONS, true)
      .build();



  // Same result as CalciteInsertDmlTest.testInsertFromTable
  @Test
  public void testInsertFromTable()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME WITHOUT ROLLUP")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testAggWithoutRollup()
  {
    final RowSignature expectedSignature =
        RowSignature.builder()
                    .add("__time", ColumnType.LONG)
                    .add("theCnt", ColumnType.LONG)
                    .add("dim1", ColumnType.STRING)
                    .build();
    final SqlSchema expectedSchema =
        SqlSchema.builder()
                 .column("__time", "TIMESTAMP(3) NOT NULL")
                 .column("theCnt", "BIGINT NOT NULL")
                 .column("dim1", "VARCHAR")
                 .build();

    testIngestionQuery()
        .sql("INSERT INTO dst\n" +
             "SELECT __time, SUM(cnt) as theCnt, dim1\n" +
             "FROM foo\n" +
             "GROUP BY __time, dim1\n" +
             "PARTITIONED BY ALL TIME\n" +
             "WITHOUT ROLLUP"
         )
        .expectTarget("dst", expectedSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            GroupByQuery.builder()
                        .setDataSource("foo")
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                            new DefaultDimensionSpec("dim1", "d1", ColumnType.STRING)
                         ))
                        .setAggregatorSpecs(
                            new LongSumAggregatorFactory("a0", "cnt")
                         )
                        .setContext(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                        .build()
        )
        .expectOutputSchema(expectedSchema)
        .verify();
  }

  @Test
  public void testAggWithRollup()
  {
    final RowSignature expectedSignature =
        RowSignature.builder()
                    .add("__time", ColumnType.LONG)
                    .add("theCnt", ColumnType.LONG)
                    .add("dim1", ColumnType.STRING)
                    .build();
    final SqlSchema expectedSchema =
        SqlSchema.builder()
                 .column("__time", "TIMESTAMP(3) NOT NULL")
                 .column("theCnt", "BIGINT")
                 .column("dim1", "VARCHAR")
                 .build();

    testIngestionQuery()
        .sql("INSERT INTO dst\n" +
             "SELECT __time, SUM(cnt) as theCnt, dim1\n" +
             "FROM foo\n" +
             "GROUP BY __time, dim1\n" +
             "PARTITIONED BY ALL TIME\n" +
             "WITH ROLLUP"
         )
        .expectTarget("dst", expectedSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            GroupByQuery.builder()
                        .setDataSource("foo")
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                            new DefaultDimensionSpec("dim1", "d1", ColumnType.STRING)
                         ))
                        .setAggregatorSpecs(
                            new LongSumAggregatorFactory("a0", "cnt")
                         )
                        .setContext(WITH_ROLLUP_CONTEXT)
                        .build()
        )
        .expectOutputSchema(expectedSchema)
        .verify();
  }

  @Test
  public void testLatestWithoutRollup()
  {
    final RowSignature expectedSignature =
        RowSignature.builder()
                    .add("__time", ColumnType.LONG)
                    .add("latestCnt", ColumnType.LONG)
                    .add("dim1", ColumnType.STRING)
                    .build();
    final SqlSchema expectedSchema =
        SqlSchema.builder()
                 .column("__time", "TIMESTAMP(3) NOT NULL")
                 .column("latestCnt", "BIGINT NOT NULL")
                 .column("dim1", "VARCHAR")
                 .build();

    testIngestionQuery()
        .sql("INSERT INTO dst\n" +
             "SELECT __time, LATEST(cnt) as latestCnt, dim1\n" +
             "FROM foo\n" +
             "GROUP BY __time, dim1\n" +
             "PARTITIONED BY ALL TIME\n" +
             "WITHOUT ROLLUP"
         )
        .expectTarget("dst", expectedSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            GroupByQuery.builder()
                        .setDataSource("foo")
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                            new DefaultDimensionSpec("dim1", "d1", ColumnType.STRING)
                         ))
                        .setAggregatorSpecs(
                            new LongLastAggregatorFactory("a0", "cnt", "__time")
                         )
                        .setContext(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                        .build()
        )
        .expectOutputSchema(expectedSchema)
        .verify();
  }

  @Test
  public void testLatestWithRollup()
  {
    final RowSignature expectedSignature =
        RowSignature.builder()
                    .add("__time", ColumnType.LONG)
                    .add("latestCnt", ColumnType.UNKNOWN_COMPLEX)
                    .add("dim1", ColumnType.STRING)
                    .build();
    final SqlSchema expectedSchema =
        SqlSchema.builder()
                 .column("__time", "TIMESTAMP(3) NOT NULL")
                 .column("latestCnt", "LATEST(BIGINT) NOT NULL")
                 .column("dim1", "VARCHAR")
                 .build();

    testIngestionQuery()
        .sql("INSERT INTO dst\n" +
             "SELECT __time, LATEST(cnt) as latestCnt, dim1\n" +
             "FROM foo\n" +
             "GROUP BY __time, dim1\n" +
             "PARTITIONED BY ALL TIME\n" +
             "WITH ROLLUP"
         )
        .expectTarget("dst", expectedSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            GroupByQuery.builder()
                        .setDataSource("foo")
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                            new DefaultDimensionSpec("dim1", "d1", ColumnType.STRING)
                         ))
                        .setAggregatorSpecs(
                            new LongLastAggregatorFactory("a0", "cnt", "__time")
                         )
                        .setContext(WITH_ROLLUP_CONTEXT)
                        .build()
        )
        .expectOutputSchema(expectedSchema)
        .verify();
  }
}
