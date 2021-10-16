package org.apache.druid.query.profile;

import java.util.ArrayList;
import java.util.List;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.QueryableIndexStorageAdapter.CursorMetrics;
import org.apache.druid.segment.filter.DimensionPredicateFilter;
import org.apache.druid.segment.filter.Filters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Represents the scan of an indexed storage, which typically means a
 * segment scan.
 */
// Order does not matter for JSON, but does for us poor humans.
@JsonPropertyOrder({"interval", "indexRows", "granularity", "vectorized", 
  "isEmpty", "indexFilters", "unknownIndexCount", "preFilteredRows", 
  "bitmapTimeNs", "filterCount", "rows", "cursors"})
public class IndexScanProfile extends OperatorProfile
{
  public static final String TYPE = "index-scan";
  
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
      profile.filterCount += Filters.count(postFilters);
    }

    @Override
    public void reportSegmentRows(long numRows)
    {
      base.reportSegmentRows(numRows);
      profile.indexRows = numRows;
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
      addFilterProfile(new PreFilterProfile(
          PreFilterProfile.BITMAP_FILTER, dimension, cardinality,
          value, bitmap.size()));
    }
    
    @Override
    public void shortCircuit(boolean value)
    {
      profile.shortCircuit = "all " + Boolean.toString(value);
    }

    @Override
    public void predicateFilter(String dimension, BitmapIndexSelector selector, DimensionPredicateFilter filter, Object bitmap)
    {
      if (bitmap == null || !(bitmap instanceof ImmutableBitmap)) {
        return;
      }
      addFilterProfile(new PreFilterProfile(
          ProfileUtils.classOf(filter), dimension,
          selector.getCardinality(dimension),
          null, ((ImmutableBitmap) bitmap).size()));
    }
    
    private void addFilterProfile(PreFilterProfile filterProfile)
    {
      if (profile.indexFilters == null) {
        profile.indexFilters = new ArrayList<>();
      }
      profile.indexFilters.add(filterProfile);
      
      // Keep track of the number of prefilters (index lookups) not explained via this call:
      // indicates a gap in the profile gathering mechanism.
      if (profile.unknownIndexCount > 0) {
        profile.unknownIndexCount--;
      }
    }
  }
  
  @JsonPropertyOrder({"type", "offsetType", "preFilterRows", "postFilterRows"})
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
  
  @JsonPropertyOrder({"kind", "dimension", "cardinality", "value", "rows"})
  public static class PreFilterProfile
  {
    public static final String BITMAP_FILTER = "bitmap";
    public static final String PREDICATE_FILTER = "predicate";
    
    /**
     * The kind of filter. "Bitmap" means a direct column bitmap, while
     * any other value is the name of the filter class which provides
     * a predicate evaluated to a bitmap.
     */
    @JsonProperty
    public String kind;
    /**
     * Name of the dimension column for this filter.
     */
    @JsonProperty
    public String dimension;
    /**
     * Cardinality of the dimension.
     */
    @JsonProperty
    public int cardinality;
    /**
     * Value for the filter. Not currently available for predicate
     * filters.
     */
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String value;
    /**
     * The number of rows which match this filter.
     */
    @JsonProperty
    public int rows;
    
    PreFilterProfile(String kind, String dimension, int cardinality, String value, int rows) {
      this.kind = kind;
      this.dimension = dimension;
      this.cardinality = cardinality;
      this.value = value;
      this.rows = rows;
    }
  }
  
  /**
   * Time interval to be scanned. This is the actual input interval
   * if isEmpty is true, else the portion of the input interval that
   * is covered by the segment.
   */
  @JsonProperty
  public String interval;
  /**
   * Number of rows in the index (segment file).
   */
  @JsonProperty
  public long indexRows;
  /**
   * True if the cursor uses the (faster) vectorized implementation.
   */
  /**
   * Rollup granularity of the query.
   */
  @JsonProperty
  public String granularity;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean descending;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean vectorized;
  /**
   * True if the time interval does not actually overlap with
   * the segment data interval: indicates no rows are returned.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isEmpty;
  /**
   * Details of the filters implemented as index lookups.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<PreFilterProfile> indexFilters;
  /**
   * Temporary value: the number of filters turned into index lookups which
   * are not accounted for in the the index filters field. Should normally
   * be 0. If non-zero, then there is another code path to implement.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int unknownIndexCount;
  /**
   * Set to "all true" or "all false" if the engine detects a filter which
   * can be evaluated as always being true or false.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String shortCircuit;
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
  public int filterCount;
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
