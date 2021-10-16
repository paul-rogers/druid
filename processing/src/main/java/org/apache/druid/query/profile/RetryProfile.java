package org.apache.druid.query.profile;

import java.util.ArrayList;
import java.util.List;

import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RetryProfile extends OperatorProfile
{
  public static final String TYPE = "retry";
  
  public static class Retry {
    @JsonProperty
    public boolean truncated;
    @JsonProperty
    public List<SegmentDescriptor> missingSegments;
    
    public Retry(ResponseContext context) {
      truncated = context.getTruncated();
      missingSegments = context.getMissingSegments();
    }
  }
  
  @JsonProperty
  public List<OperatorProfile> children = new ArrayList<>();
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Retry> retries;
  
  public void addRetry(ResponseContext context) {
    if (retries == null) {
      retries = new ArrayList<>();
    }
    retries.add(new Retry(context));
  }
}
