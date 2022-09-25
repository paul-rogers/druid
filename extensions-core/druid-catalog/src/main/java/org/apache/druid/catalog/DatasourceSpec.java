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

package org.apache.druid.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.druid.catalog.DatasourceColumnSpec.DetailColumnSpec;
import org.apache.druid.catalog.DatasourceColumnSpec.DimensionSpec;
import org.apache.druid.catalog.DatasourceColumnSpec.MeasureSpec;
import org.apache.druid.catalog.DatasourceColumnSpec.RollupColumnSpec;
import org.apache.druid.catalog.TableMetadata.TableType;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Datasource metadata exchanged via the REST API and stored
 * in the catalog.
 */
@UnstableApi
public abstract class DatasourceSpec extends TableSpec
{
  public static final String JSON_TYPE = "datasource";

  // Amazing that a parser doesn't already exist...
  private static final Map<String, Granularity> GRANULARITIES = new HashMap<>();

  static {
    GRANULARITIES.put("millisecond", Granularities.SECOND);
    GRANULARITIES.put("second", Granularities.SECOND);
    GRANULARITIES.put("minute", Granularities.MINUTE);
    GRANULARITIES.put("5 minute", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("5 minutes", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("five_minute", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("10 minute", Granularities.TEN_MINUTE);
    GRANULARITIES.put("10 minutes", Granularities.TEN_MINUTE);
    GRANULARITIES.put("ten_minute", Granularities.TEN_MINUTE);
    GRANULARITIES.put("15 minute", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("15 minutes", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("fifteen_minute", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("30 minute", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("30 minutes", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("thirty_minute", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("hour", Granularities.HOUR);
    GRANULARITIES.put("6 hour", Granularities.SIX_HOUR);
    GRANULARITIES.put("6 hours", Granularities.SIX_HOUR);
    GRANULARITIES.put("six_hour", Granularities.SIX_HOUR);
    GRANULARITIES.put("day", Granularities.DAY);
    GRANULARITIES.put("week", Granularities.WEEK);
    GRANULARITIES.put("month", Granularities.MONTH);
    GRANULARITIES.put("quarter", Granularities.QUARTER);
    GRANULARITIES.put("year", Granularities.YEAR);
    GRANULARITIES.put("all", Granularities.ALL);
  }

  /**
   * Human-readable description of the datasource.
   */
  private final String description;

  /**
   * Segment grain at ingestion and initial compaction. Aging rules
   * may override the value as segments age. If not provided here,
   * then it must be provided at ingestion time.
   */
  private final String segmentGranularity;

  /**
   * The target segment size at ingestion and initial compaction.
   * If 0, then the system setting is used.
   */
  private final int targetSegmentRows;

  private final List<ClusterKeySpec> clusterKeys;

  private final List<String> hiddenColumns;

  public DatasourceSpec(
      final String description,
      final String segmentGranularity,
      final int targetSegmentRows,
      final List<ClusterKeySpec> clusterKeys,
      final List<String> hiddenColumns,
      final Map<String, Object> tags
  )
  {
    super(tags);

    // Note: no validation here. If a bad definition got into the
    // DB, don't prevent deserialization.

    this.description = description;
    if (segmentGranularity == null) {
      this.segmentGranularity = null;
    } else {
      Granularity gran = toGranularity(segmentGranularity);
      this.segmentGranularity = gran == null ? segmentGranularity : gran.toString();
    }

    this.targetSegmentRows = targetSegmentRows;
    this.clusterKeys = clusterKeys == null || clusterKeys.isEmpty() ? null : clusterKeys;
    this.hiddenColumns  = hiddenColumns == null || hiddenColumns.isEmpty() ? null : hiddenColumns;
  }

  @Override
  public TableType type()
  {
    return TableType.DATASOURCE;
  }

  @JsonProperty("description")
  @JsonInclude(Include.NON_NULL)
  public String description()
  {
    return description;
  }

  @JsonProperty("segmentGranularity")
  @JsonInclude(Include.NON_NULL)
  public String segmentGranularity()
  {
    return segmentGranularity;
  }

  @JsonProperty("targetSegmentRows")
  @JsonInclude(Include.NON_DEFAULT)
  public int targetSegmentRows()
  {
    return targetSegmentRows;
  }

  @JsonIgnore
  public boolean hasTargetSegmentRows()
  {
    return targetSegmentRows != 0;
  }

  @JsonProperty("clusterKeys")
  @JsonInclude(Include.NON_EMPTY)
  public List<ClusterKeySpec> clusterKeys()
  {
    return clusterKeys;
  }

  @JsonProperty("hiddenColumns")
  @JsonInclude(Include.NON_EMPTY)
  public List<String> hiddenColumns()
  {
    return hiddenColumns;
  }

  private static Granularity toGranularity(String value)
  {
    return GRANULARITIES.get(StringUtils.toLowerCase(value));
  }

  /**
   * Convert a catalog granularity string to the Druid form. Catalog granularities
   * are either the usual descriptive strings (in any case), or an ISO period.
   * For the odd interval, the interval name is also accepted (for the other
   * intervals, the interval name is the descriptive string).
   */
  public static Granularity asDruidGranularity(String value)
  {
    if (Strings.isNullOrEmpty(value)) {
      return Granularities.ALL;
    }
    Granularity gran = toGranularity(value);
    if (gran != null) {
      return gran;
    }

    try {
      return new PeriodGranularity(new Period(value), null, null);
    }
    catch (IllegalArgumentException e) {
      throw new IAE(StringUtils.format("%s is an invalid period string", value));
    }
  }

  public abstract List<? extends DatasourceColumnSpec> datasourceColumns();

  @Override
  public List<? extends ColumnSpec> columnSpecs()
  {
    return datasourceColumns();
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public Builder toBuilder()
  {
    return new Builder(this);
  }

  @JsonIgnore
  public abstract boolean isDetail();

  @JsonIgnore
  public boolean isRollup()
  {
    return !isDetail();
  }

  @Override
  public void validate()
  {
    super.validate();
    if (Strings.isNullOrEmpty(segmentGranularity)) {
      throw new IAE("Segment granularity is required.");
    }
    asDruidGranularity(segmentGranularity);
    Set<String> names = new HashSet<>();
    for (DatasourceColumnSpec col : datasourceColumns()) {
      col.validate();
      if (!names.add(col.name())) {
        throw new IAE("Duplicate column name: " + col.name());
      }
    }
    if (hiddenColumns != null) {
      for (String col : hiddenColumns) {
        if (Columns.TIME_COLUMN.equals(col)) {
          throw new IAE(
              StringUtils.format("Cannot hide column %s", col)
          );
        }
      }
    }
  }

  @Override
  public String defaultSchema()
  {
    return TableId.DRUID_SCHEMA;
  }

  @Override
  public TableSpec merge(TableSpec update, Map<String, Object> raw, ObjectMapper mapper)
  {
    if (!(update instanceof DatasourceSpec)) {
      throw new IAE("The update must be of type [%s]", JSON_TYPE);
    }
    raw.remove("tags");
    raw.remove("properties");
    raw.remove("columns");
    raw.remove("hiddenColumns");
    DatasourceSpec dsUpdate = (DatasourceSpec) update;
    @SuppressWarnings("unchecked")
    Map<String, Object> asMap = mapper.convertValue(this, Map.class);
    asMap.putAll(raw);
    DatasourceSpec updatedSpec = mapper.convertValue(asMap, DatasourceSpec.class);
    Builder builder = updatedSpec.toBuilder();
    builder.tags(CatalogUtils.mergeMap(this.tags(), dsUpdate.tags()));
    builder.columns(CatalogUtils.mergeColumns(
        this.columns(),
        dsUpdate.columns(),
        (existingCol, updateCol) -> existingCol.merge(updateCol)
        )
    );
    builder.hiddenColumns(mergeHiddenColumns(this.hiddenColumns(), dsUpdate.hiddenColumns()));
    return builder.build();
  }

  private static List<String> mergeHiddenColumns(List<String> existing, List<String> revisions)
  {
    if (revisions == null) {
      return existing;
    }
    Set<String> existingSet = new HashSet<>(existing);
    List<String> revised = new ArrayList<>(existing);
    for (String col : revisions) {
      if (!existingSet.contains(col)) {
        revised.add(col);
      }
    }
    return revised;
  }

  protected boolean isEqual(DatasourceSpec other)
  {
    return Objects.equals(this.description, other.description)
        && Objects.equals(this.segmentGranularity, other.segmentGranularity)
        && Objects.equals(this.clusterKeys, other.clusterKeys)
        && this.targetSegmentRows == other.targetSegmentRows
        && Objects.equals(this.hiddenColumns, other.hiddenColumns)
        && Objects.equals(this.tags(), other.tags());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        description,
        segmentGranularity,
        clusterKeys,
        targetSegmentRows,
        hiddenColumns,
        tags()
    );
  }

  public static class DetailDatasourceSpec extends DatasourceSpec
  {

    private final List<DetailColumnSpec> columns;

    public DetailDatasourceSpec(
        @JsonProperty("description") String description,
        @JsonProperty("segmentGranularity") String segmentGranularity,
        @JsonProperty("targetSegmentRows") int targetSegmentRows,
        @JsonProperty("clusterKeys") List<ClusterKeySpec> clusterKeys,
        @JsonProperty("columns") List<DetailColumnSpec> columns,
        @JsonProperty("hiddenColumns") List<String> hiddenColumns,
        @JsonProperty("tags") Map<String, Object> tags
    )
    {
      super(
          description,
          segmentGranularity,
          targetSegmentRows,
          clusterKeys,
          hiddenColumns,
          tags
      );
      this.columns = columns == null ? Collections.emptyList() : columns;
    }

    @Override
    public boolean isDetail()
    {
      return true;
    }

    @JsonProperty("columns")
    @JsonInclude(Include.NON_EMPTY)
    public List<DetailColumnSpec> columns()
    {
      return columns;
    }

    @Override
    public List<? extends DatasourceColumnSpec> datasourceColumns()
    {
      return columns;
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == this) {
        return true;
      }
      if (o == null || o.getClass() != getClass()) {
        return false;
      }
      DetailDatasourceSpec other = (DetailDatasourceSpec) o;
      return isEqual(other)
          && Objects.equals(this.columns, other.columns);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(
          super.hashCode(),
          columns
      );
    }
  }

  public static class RollupDatasourceSpec extends DatasourceSpec
  {
    private final List<RollupColumnSpec> columns;

    /**
     * Ingestion and auto-compaction rollup granularity. If null, then no
     * rollup is enabled. Same as {@code queryGranularity} in and ingest spec,
     * but renamed since this granularity affects rollup, not queries. Can be
     * overridden at ingestion time. The grain may change as segments evolve:
     * this is the grain only for ingest.
     */
    private final String rollupGranularity;

    public RollupDatasourceSpec(
        @JsonProperty("description") final String description,
        @JsonProperty("segmentGranularity") final String segmentGranularity,
        @JsonProperty("rollupGranularity") final String rollupGranularity,
        @JsonProperty("targetSegmentRows") final int targetSegmentRows,
        @JsonProperty("clusterKeys") final List<ClusterKeySpec> clusterKeys,
        @JsonProperty("columns") final List<RollupColumnSpec> columns,
        @JsonProperty("hiddenColumns") final List<String> hiddenColumns,
        @JsonProperty("tags") final Map<String, Object> tags
    )
    {
      super(
          description,
          segmentGranularity,
          targetSegmentRows,
          clusterKeys,
          hiddenColumns,
          tags
      );
      this.rollupGranularity = rollupGranularity;
      this.columns = columns == null ? Collections.emptyList() : columns;
    }

    @Override
    public boolean isDetail()
    {
      return false;
    }

    @JsonProperty("columns")
    @JsonInclude(Include.NON_EMPTY)
    public List<RollupColumnSpec> columns()
    {
      return columns;
    }

    @JsonProperty("rollupGranularity")
    @JsonInclude(Include.NON_NULL)
    public String rollupGranularity()
    {
      return rollupGranularity;
    }

    @Override
    public List<? extends DatasourceColumnSpec> datasourceColumns()
    {
      return columns;
    }

    @Override
    public void validate()
    {
      super.validate();
      if (rollupGranularity != null) {
        asDruidGranularity(rollupGranularity);
      }
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == this) {
        return true;
      }
      if (o == null || o.getClass() != getClass()) {
        return false;
      }
      RollupDatasourceSpec other = (RollupDatasourceSpec) o;
      return isEqual(other)
          && Objects.equals(this.rollupGranularity, other.rollupGranularity)
          && Objects.equals(this.columns, other.columns);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(
          super.hashCode(),
          rollupGranularity,
          columns
      );
    }
  }

  public static class Builder
  {
    private String description;
    private String segmentGranularity;
    private String rollupGranularity;
    private int targetSegmentRows;
    private boolean enableAutoCompaction;
    private String autoCompactionDelay;
    private List<ClusterKeySpec> clusterKeys;
    private List<DatasourceColumnSpec> columns;
    private List<String> hiddenColumns;
    private Map<String, Object> tags;

    public Builder()
    {
      this.columns = new ArrayList<>();
      this.tags = new HashMap<>();
    }

    public Builder(DatasourceSpec spec)
    {
      this.description = spec.description;
      this.segmentGranularity = spec.segmentGranularity;
      this.rollupGranularity = spec.rollupGranularity;
      this.targetSegmentRows = spec.targetSegmentRows;
      this.enableAutoCompaction = spec.enableAutoCompaction;
      this.autoCompactionDelay = spec.autoCompactionDelay;
      this.tags = new HashMap<>(spec.tags());
      this.clusterKeys = spec.clusterKeys;
      this.columns = new ArrayList<>(spec.columns);
      this.hiddenColumns = spec.hiddenColumns;
    }

    public Builder description(String description)
    {
      this.description = description;
      return this;
    }

    public Builder rollupGranularity(String rollupGranularty)
    {
      this.rollupGranularity = rollupGranularty;
      return this;
    }

    public Builder segmentGranularity(String segmentGranularity)
    {
      this.segmentGranularity = segmentGranularity;
      return this;
    }

    public Builder targetSegmentRows(int targetSegmentRows)
    {
      this.targetSegmentRows = targetSegmentRows;
      return this;
    }

    public Builder enableAutoCompaction(boolean enableAutoCompaction)
    {
      this.enableAutoCompaction = enableAutoCompaction;
      return this;
    }

    public Builder autoCompactionDelay(String autoCompactionDelay)
    {
      this.autoCompactionDelay = autoCompactionDelay;
      return this;
    }

    public Builder clusterColumns(List<ClusterKeySpec> clusterKeys)
    {
      this.clusterKeys = clusterKeys;
      return this;
    }

    @VisibleForTesting
    public Builder clusterColumns(ClusterKeySpec...clusterKeys)
    {
      this.clusterKeys = Arrays.asList(clusterKeys);
      return this;
    }

    public Builder columns(List<DatasourceColumnSpec> columns)
    {
      this.columns = columns;
      return this;
    }

    public List<DatasourceColumnSpec> columns()
    {
      return columns;
    }

    public Builder column(DatasourceColumnSpec column)
    {
      if (Strings.isNullOrEmpty(column.name())) {
        throw new IAE("Column name is required");
      }
      columns.add(column);
      return this;
    }

    public Builder timeColumn()
    {
      return column("__time", "TIMESTAMP");
    }

    public Builder column(String name, String sqlType)
    {
      if (rollupGranularity == null) {
        column(new DetailColumnSpec(name, sqlType, null));
      } else {
        column(new DimensionSpec(name, sqlType, null));
      }
      return this;
    }

    public Builder measure(String name, String sqlType)
    {
      return column(new MeasureSpec(name, sqlType, null));
    }

    public Builder tags(Map<String, Object> properties)
    {
      this.tags = properties;
      return this;
    }

    public Builder tag(String key, Object value)
    {
      if (tags == null) {
        tags = new HashMap<>();
      }
      tags.put(key, value);
      return this;
    }

    public Map<String, Object> tags()
    {
      return tags;
    }

    public Builder hiddenColumns(List<String> hiddenColumns)
    {
      this.hiddenColumns = hiddenColumns;
      return this;
    }

    public DatasourceSpec build()
    {
      if (targetSegmentRows < 0) {
        targetSegmentRows = 0;
      }
      // TODO(paul): validate upper bound
      return new DatasourceSpec(
          description,
          segmentGranularity,
          rollupGranularity,
          targetSegmentRows,
          enableAutoCompaction,
          autoCompactionDelay,
          clusterKeys,
          columns,
          hiddenColumns,
          tags);
    }
  }
}
