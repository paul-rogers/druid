package org.apache.druid.query.profile;

import java.util.List;

import org.apache.druid.timeline.SegmentId;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentScanProfile extends OperatorProfile
{
  /**
   * The ID of the segment which was scanned.
   */
  @JsonProperty
  public final SegmentId segment;
  /**
   * True if the query provided no columns (which is the equivalent of the
   * SQL <code>SELECT *</code> wildcard query.)
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isWildcard;
  /**
   * The time interval within the segment to be scanned.
   */
  @JsonProperty
  public String interval;
  /**
   * True if the query was limited. For example, if other parts of the query already
   * returned enough rows to satisfy the LIMIT clause, and this scan returns no rows
   * as a result.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean limited;
  /**
   * Number of columns which this scan returns.
   */
  @JsonProperty
  public int columnCount;
  /**
   * The kind of filter, if any, applied to the rows.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String filter;
  @JsonProperty
  /**
   * The size of the batches used to return rows from this scan.
   */
  public int batchSize;
  /**
   * The number of low-level cursors used to retrieve rows for this scan.
   */
  @JsonProperty
  public int cursorCount;
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
  public List<OperatorProfile> scans;
  
  public SegmentScanProfile(SegmentId segment) {
    this.segment = segment;
  }
}
