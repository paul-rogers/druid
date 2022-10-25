package org.apache.druid.exec.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class FragmentSpec
{
  private final int sliceId;
  private final int fragmentId;
  private final int rootId;
  private final List<OperatorSpec> operators;

  @JsonCreator
  public FragmentSpec(
      @JsonProperty("sliceId") final int sliceId,
      @JsonProperty("fragmentId") final int fragmentId,
      @JsonProperty("rootId") final int rootId,
      @JsonProperty("operators") final List<OperatorSpec> operators)
  {
    this.sliceId = sliceId;
    this.fragmentId = fragmentId;
    this.rootId = rootId;
    this.operators = operators;
  }

  @JsonProperty("sliceId")
  public int sliceId()
  {
    return sliceId;
  }

  @JsonProperty("fragmentId")
  public int fragmentId()
  {
    return fragmentId;
  }

  @JsonProperty("rootId")
  public int rootId()
  {
    return rootId;
  }

  @JsonProperty("operators")
  public List<OperatorSpec> operators()
  {
    return operators;
  }
}
