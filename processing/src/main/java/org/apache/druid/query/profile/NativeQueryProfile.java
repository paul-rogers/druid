package org.apache.druid.query.profile;

import org.apache.druid.query.Query;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NativeQueryProfile extends OperatorProfile
{
  public static final String TYPE = "native";
  
  @JsonProperty
  public Query<?> query;
  
  @JsonProperty
  public OperatorProfile child;
  
  public NativeQueryProfile(Query<?> query, OperatorProfile child) {
    this.query = query;
    this.child = child;
  }
}
