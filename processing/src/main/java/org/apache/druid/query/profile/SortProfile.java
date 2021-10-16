package org.apache.druid.query.profile;

import org.apache.druid.query.scan.ScanQuery;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SortProfile extends OperatorProfile
{
  public static final String TYPE = "sort";
  
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ScanQuery.Order order;
  @JsonProperty
  public OperatorProfile child;
}
