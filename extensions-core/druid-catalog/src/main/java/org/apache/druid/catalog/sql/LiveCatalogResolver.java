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

package org.apache.druid.catalog.sql;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveColumnMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.sql.calcite.table.DruidTable;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class LiveCatalogResolver implements CatalogResolver
{
  public static final String TYPE = "catalog";

  // Copied here from MSQE since that extension is not visible here.
  public static final String CTX_ROWS_PER_SEGMENT = "msqRowsPerSegment";
  public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";

  private final MetadataCatalog catalog;

  @Inject
  public LiveCatalogResolver(final MetadataCatalog catalog)
  {
    this.catalog = catalog;
  }

  // This is a colossal hack. Should be done in the validator using a
  // Druid table, but Druid insert nodes don't use the validator.
  @Override
  public void resolveInsert(DruidSqlIngest insert, String datasource, Map<String, Object> context)
  {
    DatasourceFacade table = datasourceSpec(datasource);
    if (table == null) {
      return;
    }

    // Segment granularity
    applySegmentGranularity(insert, table);

    // Cluster keys
    applyClusterKeys(insert, table);

    // Segment size
    Integer targetSegmentRows = table.targetSegmentRows();
    if (targetSegmentRows != null) {
      context.put(CTX_ROWS_PER_SEGMENT, targetSegmentRows);
    }

    // Rollup grain
    if (table.hasRollup()) {
      // See https://druid.apache.org/docs/latest/multi-stage-query/concepts.html#rollup
      context.put(CTX_FINALIZE_AGGREGATIONS, false);
      context.put(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false);
    }
  }

  private void applySegmentGranularity(DruidSqlIngest insert, DatasourceFacade table)
  {
    // Catalog has no granularity. Accept whatever the query provides.
    String definedGranularity = table.segmentGranularity();
    if (definedGranularity == null) {
      return;
    }

    // The query has no granularity: just apply the catalog granulrity.
    if (insert.getPartitionedBy() == null) {
      insert.updateParitionedBy(
          CatalogUtils.asDruidGranularity(definedGranularity),
          definedGranularity
      );
      return;
    }

    // Both have a setting. They have to be the same.
    Granularity actual = insert.getPartitionedBy();
    if (!(actual instanceof PeriodGranularity)) {
      throw partitionByMismatchException(definedGranularity, actual.toString());
    }
    String actualGranularity = ((PeriodGranularity) actual).getPeriod().toString();
    if (!definedGranularity.equals(actualGranularity)) {
      throw partitionByMismatchException(definedGranularity, actualGranularity);
    }
  }

  private RuntimeException partitionByMismatchException(String catalogValue, String actualValue)
  {
    throw new IAE(
        "PARTITIONED BY mismatch. Catalog: [%s], query: [%s]",
        catalogValue,
        actualValue
    );
  }

  private void applyClusterKeys(DruidSqlIngest insert, DatasourceFacade table)
  {
    // Catalog has no keys. Accept whatever the query provides.
    List<ClusterKeySpec> keyCols = table.clusterKeys();
    if (keyCols == null) {
      return;
    }

    // Catalog has keys, query doesn't: just apply the catalog keys
    SqlNodeList clusterKeys = insert.getClusteredBy();
    if (clusterKeys == null || clusterKeys.getList().isEmpty()) {
      SqlNodeList keyNodes = new SqlNodeList(SqlParserPos.ZERO);
      for (ClusterKeySpec keyCol : keyCols) {
        // For now, we implicitly only support named columns as we're
        // not in a position to parse expressions here.
        SqlIdentifier colIdent = new SqlIdentifier(
            Collections.singletonList(keyCol.expr()),
            null, SqlParserPos.ZERO,
            Collections.singletonList(SqlParserPos.ZERO)
            );
        SqlNode keyNode;
        if (keyCol.desc()) {
          keyNode = SqlStdOperatorTable.DESC.createCall(SqlParserPos.ZERO, colIdent);
        } else {
          keyNode = colIdent;
        }
        keyNodes.add(keyNode);
      }
      insert.updateClusteredBy(keyNodes);
      return;
    }

    // Both the query and catalog have keys.
    if (clusterKeys.size() != keyCols.size()) {
      throw clusterKeyMismatchException(keyCols, clusterKeys);
    }
    for (int i = 0; i < clusterKeys.size(); i++) {
      ClusterKeySpec catalogKey = keyCols.get(i);
      SqlNode queryKey = clusterKeys.get(i);
      String queryKeyName;
      boolean desc;
      if (queryKey instanceof SqlIdentifier) {
        queryKeyName = ((SqlIdentifier) queryKey).getSimple();
        desc = false;
      } else if (queryKey instanceof SqlBasicCall) {
        SqlBasicCall call = (SqlBasicCall) queryKey;
        if (call.getOperands().length != 1) {
          throw clusterKeyMismatchException(keyCols, clusterKeys);
        }
        SqlNode arg1 = call.getOperands()[0];
        if (!(arg1 instanceof SqlIdentifier)) {
          throw clusterKeyMismatchException(keyCols, clusterKeys);
        }
        queryKeyName = ((SqlIdentifier) arg1).getSimple();
        desc = call.getOperator() == SqlStdOperatorTable.DESC;
      } else {
        throw clusterKeyMismatchException(keyCols, clusterKeys);
      }
      if (!catalogKey.expr().equals(queryKeyName) || catalogKey.desc() != desc) {
        throw clusterKeyMismatchException(keyCols, clusterKeys);
      }
    }
  }

  private RuntimeException clusterKeyMismatchException(List<ClusterKeySpec> keyCols, SqlNodeList clusterKeys)
  {
    // Note the format: keyCols will covert to string with brackets, clusterKeys
    // without, so we add brackets in the format only for clusterKeys
    throw new IAE(
        "CLUSTER BY mismatch. Catalog: %s, query: [%s]",
        keyCols,
        clusterKeys
    );
  }

  private DatasourceFacade datasourceSpec(String name)
  {
    TableId tableId = TableId.datasource(name);
    ResolvedTable table = catalog.resolveTable(tableId);
    if (table == null) {
      return null;
    }
    if (!DatasourceDefn.isDatasource(table)) {
      return null;
    }
    return new DatasourceFacade(table);
  }

  @Override
  public DruidTable resolveDatasource(String name, PhysicalDatasourceMetadata dsMetadata)
  {
    DruidTable table = mergeMetadata(name, dsMetadata);
    if (table != null) {
      return table;
    } else if (dsMetadata == null) {
      return null;
    } else {
      return new DatasourceTable(dsMetadata);
    }
  }

  private DruidTable mergeMetadata(
      final String name,
      @Nullable final PhysicalDatasourceMetadata dsMetadata
  )
  {
    DatasourceFacade dsSpec = datasourceSpec(name);
    if (dsSpec == null) {
      return null;
    }
    if (dsSpec.columns().isEmpty()) {
      return null;
    }

    if (dsMetadata == null) {
      return emptyDatasource(name, dsSpec);
    } else {
      return mergeDatasource(name, dsMetadata, dsSpec);
    }
  }

  private DruidTable emptyDatasource(String name, DatasourceFacade dsSpec)
  {
    RowSignature.Builder builder = RowSignature.builder();
    Map<String, EffectiveColumnMetadata> columns = new HashMap<>();
    boolean hasTime = false;
    for (ColumnSpec col : dsSpec.columns()) {
      EffectiveColumnMetadata colMetadata = columnFromCatalog(col, null);
      if (colMetadata.name().equals(Columns.TIME_COLUMN)) {
        hasTime = true;
      }
      builder.add(col.name(), colMetadata.druidType());
      columns.put(col.name(), colMetadata);
    }
    if (!hasTime) {
      columns.put(Columns.TIME_COLUMN, new EffectiveColumnMetadata(
          Columns.TIME_COLUMN,
          ColumnType.LONG
      ));
      builder = RowSignature.builder()
          .add(Columns.TIME_COLUMN, ColumnType.LONG)
          .addAll(builder.build());
    }

    final PhysicalDatasourceMetadata mergedMetadata = new PhysicalDatasourceMetadata(
          new TableDataSource(name),
          builder.build(),
          true, // Can join to an empty table
          false // Cannot broadcast an empty table
    );
    return new DatasourceTable(
        mergedMetadata.rowSignature(),
        mergedMetadata,
        new EffectiveMetadata(columns, true)
    );
  }

  private EffectiveColumnMetadata columnFromCatalog(ColumnSpec col, ColumnType physicalType)
  {
    ColumnType type = Columns.druidType(col.sqlType());
    if (type != null) {
      // Use the type that the user provided.
    } else if (physicalType == null) {
      // Corner case: the user has defined a column in the catalog, has
      // not specified a type (meaning the user wants Druid to decide), but
      // there is no data at this moment. Guess String as the type for the
      // null values. If new segments appear between now and execution, we'll
      // convert the values to string, which is always safe.
      type = ColumnType.STRING;
    } else {
      type = physicalType;
    }
    return new EffectiveColumnMetadata(col.name(), type);
  }

  private DruidTable mergeDatasource(
      final String name,
      final PhysicalDatasourceMetadata dsMetadata,
      final DatasourceFacade dsSpec)
  {
    Set<String> physicalCols = new HashSet<>();
    final RowSignature physicalSchema = dsMetadata.rowSignature();
    for (Entry<String, ColumnType> entry : physicalSchema.entries()) {
      physicalCols.add(entry.getKey());
    }

    // Merge columns. All catalog-defined columns come first,
    // in the order defined in the catalog.
    final RowSignature.Builder builder = RowSignature.builder();
    Map<String, EffectiveColumnMetadata> columns = new HashMap<>();
    for (ColumnSpec col : dsSpec.columns()) {
      ColumnType physicalType = null;
      if (physicalCols.remove(col.name())) {
        physicalType = dsMetadata.rowSignature().getColumnType(col.name()).get();
      }
      EffectiveColumnMetadata colMetadata = columnFromCatalog(col, physicalType);
      builder.add(col.name(), colMetadata.druidType());
      columns.put(col.name(), colMetadata);
    }

    // Mark any hidden columns. Assumes that the hidden columns are a disjoint set
    // from the defined columns.
    if (dsSpec.hiddenColumns() != null) {
      for (String colName : dsSpec.hiddenColumns()) {
        physicalCols.remove(colName);
      }
    }

    // Any remaining columns follow, if not marked as hidden
    // in the catalog.
    for (int i = 0; i < physicalSchema.size(); i++) {
      String colName = physicalSchema.getColumnName(i);
      if (!physicalCols.contains(colName)) {
        continue;
      }
      ColumnType physicalType = dsMetadata.rowSignature().getColumnType(colName).get();
      EffectiveColumnMetadata colMetadata = EffectiveColumnMetadata.fromPhysical(colName, physicalType);
      columns.put(colName, colMetadata);
      builder.add(colName, physicalType);
    }

    EffectiveMetadata effectiveMetadata = EffectiveMetadata.fromPhysical(columns);
    return new DatasourceTable(builder.build(), dsMetadata, effectiveMetadata);
  }
}
