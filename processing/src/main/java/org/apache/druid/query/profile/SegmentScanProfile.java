package org.apache.druid.query.profile;

import java.util.List;

import org.apache.druid.timeline.SegmentId;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"segment", "interval", "columnCount", "isWildcard", 
  "batchSize", "limited", "cursors", "rows", "error"})
public class SegmentScanProfile extends OperatorProfile
{
  public static final String TYPE = "segment-scan";
  
  /**
   * The ID of the segment which was scanned.
   */
  @JsonProperty
  public final SegmentId segment;
  /**
   * The time interval within the segment to be scanned.
   */
  @JsonProperty
  public String interval;
  /**
   * Number of columns which this scan returns.
   */
  @JsonProperty
  public int columnCount;
  /**
   * True if the query provided no columns (which is the equivalent of the
   * SQL <code>SELECT *</code> wildcard query.)
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isWildcard;
  /**
   * The size of the batches used to return rows from this scan.
   */
  @JsonProperty
  public int batchSize;
  /**
   * True if the query was limited. For example, if other parts of the query already
   * returned enough rows to satisfy the LIMIT clause, and this scan returns no rows
   * as a result.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean limited;
  /**
   * The number of rows returned from this scan.
   */
  @JsonProperty
  public long rows;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String error;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<OperatorProfile> cursors;
  
  public SegmentScanProfile(SegmentId segment) {
    this.segment = segment;
  }
}
