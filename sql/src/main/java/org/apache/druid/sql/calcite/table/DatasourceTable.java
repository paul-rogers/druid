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

package org.apache.druid.sql.calcite.table;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.planner.DruidMetadataResolver;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a SQL table that models a Druid datasource.
 * <p>
 * Once the catalog code is merged, this class will combine physical information
 * from the segment cache with logical information from the catalog to produce
 * the SQL-user's view of the table. The resulting merged view is used to plan
 * queries and is the source of table information in the {@code INFORMATION_SCHEMA}.
 */
public class DatasourceTable extends DruidTable
{
  /**
   * The physical metadata for a datasource, derived from the list of segments
   * published in the Coordinator. Used only for datasources, since only
   * datasources are computed from segments.
   */
  public static class PhysicalDatasourceMetadata
  {
    private final TableDataSource dataSource;
    private final RowSignature rowSignature;
    private final boolean joinable;
    private final boolean broadcast;
    private final Map<String, AggregatorFactory> aggregators;

    public PhysicalDatasourceMetadata(
        final TableDataSource dataSource,
        final RowSignature rowSignature,
        final boolean isJoinable,
        final boolean isBroadcast,
        final Map<String, AggregatorFactory> aggregators
    )
    {
      this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
      this.rowSignature = Preconditions.checkNotNull(rowSignature, "rowSignature");
      this.joinable = isJoinable;
      this.broadcast = isBroadcast;
      this.aggregators = aggregators;
    }

    public TableDataSource dataSource()
    {
      return dataSource;
    }

    public RowSignature rowSignature()
    {
      return rowSignature;
    }

    public boolean isJoinable()
    {
      return joinable;
    }

    public boolean isBroadcast()
    {
      return broadcast;
    }

    public Map<String, AggregatorFactory> aggregators()
    {
      return aggregators;
    }

    public EffectiveMetadata toEffectiveMetadata()
    {
      Map<String, EffectiveColumnMetadata> columns = new HashMap<>();
      for (int i = 0; i < rowSignature.size(); i++) {
        String colName = rowSignature.getColumnName(i);
        ColumnType colType = rowSignature.getColumnType(i).get();
        AggregatorFactory agg = aggregators == null ? null : aggregators.get(colName);

        EffectiveColumnMetadata colMetadata = EffectiveColumnMetadata.fromPhysical(colName, colType, agg);
        columns.put(colName, colMetadata);
      }
      return EffectiveMetadata.fromPhysical(aggregators != null && !aggregators.isEmpty(), columns);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      PhysicalDatasourceMetadata that = (PhysicalDatasourceMetadata) o;

      if (!Objects.equals(dataSource, that.dataSource)) {
        return false;
      }
      return Objects.equals(rowSignature, that.rowSignature);
    }

    @Override
    public int hashCode()
    {
      int result = dataSource != null ? dataSource.hashCode() : 0;
      result = 31 * result + (rowSignature != null ? rowSignature.hashCode() : 0);
      return result;
    }

    @Override
    public String toString()
    {
      return "DatasourceMetadata{" +
             "dataSource=" + dataSource +
             ", rowSignature=" + rowSignature +
             '}';
    }
  }

  public enum ColumnKind
  {
    TIME,
    DETAIL,
    DIMENSION,
    MEASURE
  }

  public abstract static class EffectiveColumnMetadata
  {
    protected final String name;
    protected final ColumnType type;

    public EffectiveColumnMetadata(String name, ColumnType type)
    {
      this.name = name;
      this.type = type;
    }

    public String name()
    {
      return name;
    }

    public ColumnType druidType()
    {
      return type;
    }

    public abstract ColumnKind kind();

    public static EffectiveColumnMetadata fromPhysical(String name, ColumnType type, AggregatorFactory agg)
    {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(name)) {
        return new EffectiveDimensionMetadata(name, type, ColumnKind.TIME);
      } else if (agg == null) {
        return new EffectiveDimensionMetadata(name, type, ColumnKind.DIMENSION);
      } else {
        return new EffectiveMeasureMetadata(name, type, agg);
      }
    }

    protected SqlNode rewriteForSelect(
        SqlParserPos posn,
        SqlIdentifier identifier,
        DruidMetadataResolver metadataResolver
    )
    {
      return null;
    }
  }

  public static class EffectiveDimensionMetadata extends EffectiveColumnMetadata
  {
    private final ColumnKind kind;

    public EffectiveDimensionMetadata(String name, ColumnType type, ColumnKind kind)
    {
      super(name, type);
      this.kind = kind;
    }

    @Override
    public ColumnKind kind()
    {
      return kind;
    }

    @Override
    public String toString()
    {
      return "Dimension{" +
          "name=" + name +
          ", kind=" + kind.name() +
          ", type=" + type.asTypeString() +
          "}";
    }
  }

  public static class EffectiveMeasureMetadata extends EffectiveColumnMetadata
  {
    private final AggregatorFactory aggFactory;

    public EffectiveMeasureMetadata(String name, ColumnType type, AggregatorFactory aggFactory)
    {
      super(name, type);
      this.aggFactory = aggFactory;
    }

    @Override
    public ColumnKind kind()
    {
      return ColumnKind.MEASURE;
    }

    @Override
    public String toString()
    {
      return "Measure{" +
          "name=" + name +
          ", type=" + type.asTypeString() +
          ", agg=" + aggFactory.toString() +
          "}";
    }

    @Override
    public SqlNode rewriteForSelect(
        SqlParserPos posn,
        SqlIdentifier identifier,
        DruidMetadataResolver metadataResolver
    )
    {
      SqlAggregator agg = metadataResolver.aggregatorForFactory(aggFactory.getClass());
      if (agg == null) {
        return null;
      }
      // TODO: Ensure the node is properly validated for this point in time.
      return agg.calciteFunction().createCall(posn, identifier);
    }
  }

  public abstract static class EffectiveMetadata
  {
    private final boolean isEmpty;
    private final Map<String, EffectiveColumnMetadata> columns;


    public EffectiveMetadata(Map<String, EffectiveColumnMetadata> columns, boolean isEmpty)
    {
      this.isEmpty = isEmpty;
      this.columns = columns;
    }

    public static EffectiveMetadata fromPhysical(boolean isAggregated, Map<String, EffectiveColumnMetadata> columns)
    {
      if (isAggregated) {
        return new EffectiveRollupMetadata(columns, false);
      } else {
        return new EffectiveDetailMetadata(columns, false);
      }
    }

    public EffectiveColumnMetadata column(String name)
    {
      return columns.get(name);
    }

    @Override
    public String toString()
    {
      return getClass().getSimpleName() + "{" +
          "empty=" + Boolean.toString(isEmpty) +
          ", columns=" + columns.toString() +
          "}";

    }
  }

  public static class EffectiveDetailMetadata extends EffectiveMetadata
  {
    public EffectiveDetailMetadata(Map<String, EffectiveColumnMetadata> columns, boolean isEmpty)
    {
      super(columns, isEmpty);
    }
  }

  public static class EffectiveRollupMetadata extends EffectiveMetadata
  {
    public EffectiveRollupMetadata(Map<String, EffectiveColumnMetadata> columns, boolean isEmpty)
    {
      super(columns, isEmpty);
    }
  }

  private final PhysicalDatasourceMetadata physicalMetadata;
  private final EffectiveMetadata effectiveMetadata;

  public DatasourceTable(
      final PhysicalDatasourceMetadata physicalMetadata
  )
  {
    this(
        physicalMetadata.rowSignature(),
        physicalMetadata,
        physicalMetadata.toEffectiveMetadata());
  }

  public DatasourceTable(
      final RowSignature rowSignature,
      final PhysicalDatasourceMetadata physicalMetadata,
      final EffectiveMetadata effectiveMetadata
  )
  {
    super(rowSignature);
    this.physicalMetadata = physicalMetadata;
    this.effectiveMetadata = effectiveMetadata;
  }

  @Override
  public DataSource getDataSource()
  {
    return physicalMetadata.dataSource();
  }

  @Override
  public boolean isJoinable()
  {
    return physicalMetadata.isJoinable();
  }

  @Override
  public boolean isBroadcast()
  {
    return physicalMetadata.isBroadcast();
  }

  public EffectiveMetadata effectiveMetadata()
  {
    return effectiveMetadata;
  }

  @Override
  public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable table)
  {
    return LogicalTableScan.create(context.getCluster(), table);
  }

  public SqlNode rewriteSelectColumn(
      SqlParserPos posn,
      SqlIdentifier identifier,
      DruidMetadataResolver metadataResolver
  )
  {
    EffectiveColumnMetadata col = effectiveMetadata.column(Util.last(identifier.names));
    if (col == null) {
      return null;
    }
    return col.rewriteForSelect(posn, identifier, metadataResolver);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return physicalMetadata.equals(o);
  }

  @Override
  public int hashCode()
  {
    return physicalMetadata.hashCode();
  }

  @Override
  public String toString()
  {
    return "DruidTable{physicalMetadata=" +
           physicalMetadata == null ? "null" : physicalMetadata.toString() +
           ", effectiveMetadata=" + effectiveMetadata.toString() +
           '}';
  }
}
