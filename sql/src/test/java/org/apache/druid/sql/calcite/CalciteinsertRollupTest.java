package org.apache.druid.sql.calcite;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.junit.Test;

public class CalciteinsertRollupTest extends CalciteIngestionDmlTest
{
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
}
