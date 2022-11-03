package org.apache.druid.exec.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.column.ColumnType;

import java.util.List;

public class WindowSpec extends AbstractUnarySpec
{
  public static class OutputColumn
  {
    @JsonProperty("name")
    public final String name;
    @JsonProperty("dataType")
    public final ColumnType dataType;

    @JsonCreator
    public OutputColumn(
        final String name,
        final ColumnType dataType
    )
    {
      this.name = name;
      this.dataType = dataType;
    }
  }

  public static class CopyProjection extends OutputColumn
  {
    @JsonProperty("source")
    public final String sourceColumn;

    @JsonCreator
    public CopyProjection(
        @JsonProperty("name") final String name,
        @JsonProperty("dataType") final ColumnType dataType,
        @JsonProperty("source") final String sourceColumn
    )
    {
      super(name, dataType);
      this.sourceColumn = sourceColumn;
    }
  }

  public static class SimpleExpression extends OutputColumn
  {
    @JsonProperty("expression")
    public final String expression;

    @JsonCreator
    public SimpleExpression(
        @JsonProperty("name") final String name,
        @JsonProperty("dataType") final ColumnType dataType,
        @JsonProperty("expression") final String expression

    )
    {
      super(name, dataType);
      this.expression = expression;
    }
  }

  public static class OffsetExpression extends OutputColumn
  {
    @JsonProperty("offset")
    public final int offset;
    @JsonProperty("expression")
    public final String expression;

    @JsonCreator
    public OffsetExpression(
        @JsonProperty("name") final String name,
        @JsonProperty("dataType") final ColumnType dataType,
        @JsonProperty("expression") final String expression,
        @JsonProperty("offset") final int offset

    )
    {
      super(name, dataType);
      this.offset = offset;
      this.expression = expression;
    }
  }

  @JsonProperty("batchSize")
  public final int batchSize;
  @JsonProperty("columns")
  public final List<OutputColumn> columns;
  @JsonProperty("partitionKeys")
  @JsonInclude(Include.NON_EMPTY)
  public final List<String> partitionKeys;

  @JsonCreator
  public WindowSpec(
      @JsonProperty("id") final int id,
      @JsonProperty("child") final int child,
      @JsonProperty("batchSize") final int batchSize,
      @JsonProperty("columns") final List<OutputColumn> columns,
      @JsonProperty("partitionKeys") final List<String> partitionKeys
  )
  {
    super(id, child);
    this.batchSize = batchSize;
    this.columns = columns;
    this.partitionKeys = partitionKeys;
  }
}
