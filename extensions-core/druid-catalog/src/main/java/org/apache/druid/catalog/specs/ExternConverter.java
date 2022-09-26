package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;

/**
 * Builds the components of Druid external data source from a
 * {@link TableSpec} using JSON object conversion. The conversion can
 * also be done from named SQL function arguments by first mapping the
 * arguments to a temporary {@code TableSpec}.
 */
public class ExternConverter
{
  public ExternalSpec convert(ResolvedTable table)
  {

  }

  public class ExternalSpec
  {
    protected final InputSource inputSource;
    protected final InputFormat inputFormat;
    protected final RowSignature signature;

    @JsonCreator
    public ExternalSpec(
        @JsonProperty("inputSource") final InputSource inputSource,
        @JsonProperty("inputFormat") final InputFormat inputFormat,
        @JsonProperty("signature") final RowSignature signature)
    {
      this.inputSource = inputSource;
      this.inputFormat = inputFormat;
      this.signature = signature;
    }

    public InputSource inputSource()
    {
      return inputSource;
    }

    public InputFormat inputFormat()
    {
      return inputFormat;
    }

    public RowSignature signature()
    {
      return signature;
    }
  }
}
