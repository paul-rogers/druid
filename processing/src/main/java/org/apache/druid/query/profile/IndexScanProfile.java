package org.apache.druid.query.profile;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IndexScanProfile extends OperatorProfile
{
  public static String EMPTY_SCAN = "empty";
  
  public static class CursorProfile {
    public static String INDEX = "index";
    public static String VECTOR_INDEX = "vector-index";
    
    @JsonProperty
    public String type;
    public int preFilterRows;
    public int postFilterRows;
  }
  
  @JsonProperty
  public boolean vectorized;
  @JsonProperty
  public String interval;
  @JsonProperty
  public String type;
  @JsonProperty
  public boolean hasPreFilter;
  @JsonProperty
  public String postFilter;
  @JsonProperty
  public int indexRows;
  @JsonProperty
  public String granularity;
  @JsonProperty
  public boolean descending;
  @JsonProperty
  public List<CursorProfile> cursors;
}
