package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;

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