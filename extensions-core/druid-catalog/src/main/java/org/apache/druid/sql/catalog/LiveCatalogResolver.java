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

package org.apache.druid.sql.catalog;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.catalog.specs.ClusterKeySpec;
import org.apache.druid.catalog.specs.Columns;
import org.apache.druid.catalog.specs.TableId;
import org.apache.druid.catalog.storage.DatasourceColumnSpec;
import org.apache.druid.catalog.storage.DatasourceSpec;
import org.apache.druid.catalog.storage.TableMetadata;
import org.apache.druid.catalog.storage.DatasourceColumnSpec.MeasureSpec;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.parser.BaseDruidSqlInsert;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DatasourceTable.ColumnKind;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveColumnMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveDetailMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveDimensionMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveMeasureMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveRollupMetadata;
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

  private final MetadataCatalog catalog;

  @Inject
  public LiveCatalogResolver(final MetadataCatalog catalog)
  {
    this.catalog = catalog;
  }

  // This is a colossal hack. Should be done in the validator using a
  // Druid table, but Druid insert nodes don't use the validator.
  @Override
  public void resolveInsert(BaseDruidSqlInsert insert, String datasource, Map<String, Object> context)
  {
    DatasourceSpec dsSpec = datasourceSpec(datasource);
    if (dsSpec == null) {
      return;
    }

    // Segment granularity
    if (insert.getPartitionedBy() == null) {
      insert.updateParitionedBy(
          DatasourceSpec.asDruidGranularity(dsSpec.segmentGranularity()),
          dsSpec.segmentGranularity());
    }

    // Cluster keys
    SqlNodeList clusterKeys = insert.getClusteredBy();
    if (clusterKeys == null || clusterKeys.getList().isEmpty()) {
      List<ClusterKeySpec> keyCols = dsSpec.clusterKeys();
      if (keyCols != null) {
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
      }
    }

    // Segment size
    if (dsSpec.hasTargetSegmentRows()) {
      context.put(CTX_ROWS_PER_SEGMENT, dsSpec.targetSegmentRows());
    }
  }

  private DatasourceSpec datasourceSpec(String name)
  {
    TableId tableId = TableId.datasource(name);
    TableMetadata table = catalog.resolveTable(tableId);
    if (table == null) {
      return null;
    }
    if (!(table.spec() instanceof DatasourceSpec)) {
      return null;
    }
    return (DatasourceSpec) table.spec();
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
    DatasourceSpec dsSpec = datasourceSpec(name);
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

  private DruidTable emptyDatasource(String name, DatasourceSpec dsSpec)
  {
    RowSignature.Builder builder = RowSignature.builder();
    Map<String, EffectiveColumnMetadata> columns = new HashMap<>();
    boolean isAggTable = dsSpec.isRollup();
    boolean hasTime = false;
    for (DatasourceColumnSpec col : dsSpec.columns()) {
      EffectiveColumnMetadata colMetadata = columnFromCatalog(isAggTable, col, null);
      if (colMetadata.kind() == ColumnKind.TIME) {
        hasTime = true;
      }
      builder.add(col.name(), colMetadata.druidType());
      columns.put(col.name(), colMetadata);
    }
    if (!hasTime) {
      columns.put(Columns.TIME_COLUMN, new EffectiveDimensionMetadata(
          Columns.TIME_COLUMN,
          ColumnType.LONG,
          ColumnKind.TIME
      ));
      builder = RowSignature.builder()
          .add(Columns.TIME_COLUMN, ColumnType.LONG)
          .addAll(builder.build());
    }

    final PhysicalDatasourceMetadata mergedMetadata = new PhysicalDatasourceMetadata(
          new TableDataSource(name),
          builder.build(),
          true, // Can join to an empty table
          false, // Cannot broadcast an empty table
          null
    );
    EffectiveMetadata effectiveMetadata = isAggTable
        ? new EffectiveRollupMetadata(columns, true)
        : new EffectiveDetailMetadata(columns, true);
    return new DatasourceTable(
        mergedMetadata.rowSignature(),
        mergedMetadata,
        effectiveMetadata);
  }

  private EffectiveColumnMetadata columnFromCatalog(boolean isAggTable, DatasourceColumnSpec col, ColumnType physicalType)
  {
    ColumnType type = col.druidType();
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
    if (isAggTable && col instanceof MeasureSpec) {
      return new EffectiveMeasureMetadata(col.name(), type, null);
    }
    final ColumnKind kind;
    if (Columns.isTimeColumn(col.name())) {
      kind = ColumnKind.TIME;
    } else if (isAggTable) {
      kind = ColumnKind.DIMENSION;
    } else {
      kind = ColumnKind.DETAIL;
    }
    return new EffectiveDimensionMetadata(col.name(), type, kind);
  }

  private DruidTable mergeDatasource(
      final String name,
      final PhysicalDatasourceMetadata dsMetadata,
      final DatasourceSpec dsSpec)
  {
    boolean isAggTable = dsSpec.isRollup();
    Set<String> physicalCols = new HashSet<>();
    final RowSignature physicalSchema = dsMetadata.rowSignature();
    for (Entry<String, ColumnType> entry : physicalSchema.entries()) {
      physicalCols.add(entry.getKey());
    }

    // Merge columns. All catalog-defined columns come first,
    // in the order defined in the catalog.
    final RowSignature.Builder builder = RowSignature.builder();
    Map<String, EffectiveColumnMetadata> columns = new HashMap<>();
    for (DatasourceColumnSpec col : dsSpec.columns()) {
      ColumnType type = col.druidType();
      ColumnType physicalType = null;
      if (physicalCols.remove(col.name())) {
        physicalType = dsMetadata.rowSignature().getColumnType(col.name()).get();
      }
      EffectiveColumnMetadata colMetadata = columnFromCatalog(isAggTable, col, physicalType);
      builder.add(col.name(), type);
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
    Map<String, AggregatorFactory> aggregators = dsMetadata.aggregators();
    for (int i = 0; i < physicalSchema.size(); i++) {
      String colName = physicalSchema.getColumnName(i);
      if (!physicalCols.contains(colName)) {
        continue;
      }
      ColumnType physicalType = dsMetadata.rowSignature().getColumnType(colName).get();
      AggregatorFactory agg = aggregators == null ? null : aggregators.get(colName);

      EffectiveColumnMetadata colMetadata = EffectiveColumnMetadata.fromPhysical(colName, physicalType, agg);
      columns.put(colName, colMetadata);
      builder.add(colName, physicalType);
    }

    EffectiveMetadata effectiveMetadata = EffectiveMetadata.fromPhysical(aggregators != null && !aggregators.isEmpty(), columns);
    return new DatasourceTable(builder.build(), dsMetadata, effectiveMetadata);
  }
}
