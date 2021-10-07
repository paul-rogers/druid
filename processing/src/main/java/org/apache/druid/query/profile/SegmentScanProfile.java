package org.apache.druid.query.profile;

import org.apache.druid.timeline.SegmentId;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentScanProfile extends OperatorProfile
{
  private final SegmentId segment;
  @JsonProperty
  public long rowCount;
  @JsonProperty
  public boolean limited;
  @JsonProperty
  public int columnCount;
  @JsonProperty
  public String filter;
  @JsonProperty
  public int batchSize;
  @JsonProperty
  public int cursorCount;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String error;
  
  public SegmentScanProfile(SegmentId segment) {
    this.segment = segment;
  }
  
  @JsonProperty
  public SegmentId getSegment() {
    return segment;
  }
}
