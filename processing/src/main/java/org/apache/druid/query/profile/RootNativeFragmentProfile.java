package org.apache.druid.query.profile;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.druid.query.Query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class RootNativeFragmentProfile extends FragmentProfile
{
  /**
   * Optional address of the client which sent the query.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public final String remoteAddress;
  /**
   * Columns required to process the query.
   */
  @JsonProperty
  public final List<String> columns;
  /**
   * QueryID assigned to the query by the receiving host.
   */
  @JsonProperty
  public final String queryId;
  /**
   * Native query as received by the host without query ID or context.
   */
  @JsonProperty
  public final Query<?> query;
  
  @JsonCreator
  public RootNativeFragmentProfile(
      @JsonProperty("host") @Nullable String host,
      @JsonProperty("service") @Nullable String service,
      @JsonProperty("remoteAddress") @Nullable String remoteAddress,
      @JsonProperty("queryID") String queryId,
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("startTime") long startTime,
      @JsonProperty("timeNs") long timeNs,
      @JsonProperty("cpuNs") long cpuNs,
      @JsonProperty("rows") long rows,
      @JsonProperty("rootOperator") OperatorProfile rootOperator
  )
  {
    super(host, service, startTime, timeNs, cpuNs, rows, rootOperator);
    this.queryId = queryId;
    this.query = query;
    this.columns = columns;
    this.remoteAddress = remoteAddress;
  }

  public static boolean isSameQueryType(Query<?> query1, Query<?> query2) {
    if (query1 == null && query2 == null) {
      return true;
    }
    if (query1 == null || query2 == null) {
      return false;
    }
    return query1.getClass() == query2.getClass() ;
  }
  
  /**
   * Primarily for testing. Ensures that the scalar fields are equal,
   * does not do a deep compare of operators.
   */
  @Override
  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof ChildFragmentProfile)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RootNativeFragmentProfile other = (RootNativeFragmentProfile) o;
    return Objects.equal(remoteAddress, other.remoteAddress) &&
        Objects.equal(queryId, other.queryId) &&
        // Used only for testing: checking the type is sufficient
        isSameQueryType(query, other.query) &&
        Objects.equal(columns, other.columns);
  }
  
  @Override
  public String toString() {
    return toStringHelper()
        .add("remoteAddress", remoteAddress)
        .add("queryId", queryId)
        .add("query", query)
        .add("columns", columns)
        .toString();
  }
}
