package org.apache.druid.query.profile;

import java.util.ArrayList;
import java.util.List;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.QueryableIndexStorageAdapter.CursorMetrics;
import org.apache.druid.segment.filter.Filters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the scan of an indexed storage, which typically means a
 * segment scan.
 */
public class IndexScanProfile extends OperatorProfile
{
  public static class CursorProfileMetrics implements CursorMetrics
  { 
    private final IndexScanProfile profile;
    private final CursorMetrics base;

    public CursorProfileMetrics(IndexScanProfile profile, QueryMetrics<?> queryMetrics)
    {
      this.profile = profile;
      this.base = CursorMetrics.of(queryMetrics);
    }

    @Override
    public boolean isLive()
    {
      return true;
    }

    @Override
    public void preFilters(List<Filter> preFilters)
    {
      if (preFilters != null) {
        profile.unknownIndexCount = preFilters.size();
      }
      base.preFilters(preFilters);
    }

    @Override
    public void postFilters(List<Filter> postFilters)
    {
      base.postFilters(postFilters);
      profile.postFilterCount += Filters.count(postFilters);
    }

    @Override
    public void reportSegmentRows(long numRows)
    {
      base.reportSegmentRows(numRows);
      profile.segmentRows = numRows;
    }

    @Override
    public void reportPreFilteredRows(long numRows)
    {
      base.reportPreFilteredRows(numRows);
      profile.preFilteredRows = numRows;
    }

    @Override
    public BitmapResultFactory<?> makeBitmapResultFactory(BitmapFactory factory)
    {
      final BitmapResultFactory<?> resultFactory;
      if (base.isLive()) {
        resultFactory = base.makeBitmapResultFactory(factory);
      } else {
        resultFactory = new DefaultBitmapResultFactory(factory);
      }
      return resultFactory;
    }

    @Override
    public void reportBitmapConstructionTime(long timeNs)
    {
      base.reportBitmapConstructionTime(timeNs);
      profile.bitmapTimeNs = timeNs;
    }

    @Override
    public void bitmapIndex(String dimension, int cardinality, String value, ImmutableBitmap bitmap)
    {
      if (profile.preFilters == null) {
        profile.preFilters = new ArrayList<>();
      }
      profile.preFilters.add(new PreFilterProfile(dimension, cardinality, value, bitmap.size()));
      
      // Keep track of the number of prefilters (index lookups) not explained via this call:
      // indicates a gap in the profile gathering mechanism.
      if (profile.unknownIndexCount > 0) {
        profile.unknownIndexCount--;
      }
    }
  }
  
  public static class CursorProfile
  {
    public static String INDEX = "index";
    public static String VECTOR_INDEX = "vector-index";
    
    @JsonProperty
    public String type;
    @JsonProperty
    public String offsetType;
    @JsonProperty
    public int preFilterRows;
    @JsonProperty
    public int postFilterRows;
  }
  
  public static class PreFilterProfile
  {
    @JsonProperty
    public String dimension;
    @JsonProperty
    public int cardinality;
    @JsonProperty
    public String value;
    @JsonProperty
    public int rows;
    
    PreFilterProfile(String dimension, int cardinality, String value, int rows) {
      this.dimension = dimension;
      this.cardinality = cardinality;
      this.value = value;
      this.rows = rows;
    }
  }
  
  /**
   * True if the cursor uses the (faster) vectorized implementation.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean vectorized;
  /**
   * Time interval to be scanned. This is the actual input interval
   * if isEmpty is true, else the portion of the input interval that
   * is covered by the segment.
   */
  @JsonProperty
  public String interval;
  /**
   * True if the time interval does not actually overlap with
   * the segment data interval: indicates no rows are returned.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isEmpty;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int unknownIndexCount;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<PreFilterProfile> preFilters;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int postFilterCount;
  /**
   * The total number of rows in this index (segment file).
   */
  @JsonProperty
  public int indexRows;
  /**
   * Rollup granularity of the query.
   */
  @JsonProperty
  public String granularity;
  @JsonProperty
  public long segmentRows;
  /**
   * The number of rows after applying "pre filters": those that can be resolved
   * by using column bitmap indexes. This is "pre-filtered" because this is
   * the number of rows which go into the per-row filters. Column-level filtering
   * is faster than row-level filtering.
   * <p>
   * Selectivity of the column-level filters is just
   * <code>preFilteredRows / indexRows</code>.
   */
  @JsonProperty
  public long preFilteredRows;
  /**
   * The amount of time, in ns, taken to compute the intersection of all the
   * column level index bitmaps.
   */
  @JsonProperty
  public long bitmapTimeNs;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean descending;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<CursorProfile> cursors;
  
  public CursorProfile addCursor() {
    if (cursors == null) {
      cursors = new ArrayList<>();
    }
    CursorProfile cursor = new CursorProfile();
    cursors.add(cursor);
    return cursor;
  }
}
