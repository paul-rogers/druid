package org.apache.druid.sql.calcite;

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
}
