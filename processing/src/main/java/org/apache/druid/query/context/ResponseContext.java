/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.context;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.query.MultiQueryMetricsCollector;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.profile.OperatorProfile;
import org.apache.druid.query.profile.OperatorProfile.OpaqueOperator;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The context for storing and passing data between chains of {@link org.apache.druid.query.QueryRunner}s.
 * The context is also transferred between Druid nodes with all the data it contains.
 * <p>
 * The response context consists of a set of key/value pairs. Keys are those defined in
 * the <code>Keys</code> registry. Keys are indexed by key instance, not by name. The
 * key defines the type of the associated value, including logic to merge values and
 * to deserialize JSON values for that key.
 *
 * <h4>Structure</h4>
 * The context has evolved to perform multiple tasks. First, it holds three kinds
 * of information:
 * <ul>
 * <li>Information to be returned in the query response header.
 * (These are values tagged as <code>HEADER_AND_TRAILER</code>.)</li>
 * <li>Values passed within a single server. These are tagged with
 * visibility <code>NONE</code>.)</li>
 * <li>Values returned in the trailer (AKA "query profile.") These are
 * marked with <code>TRAILER</code>.)</li>
 * </ul>
 * Second, it performs multiple tasks:
 * <ul>
 * <li>Registers the keys to be used in the header. But, since it also holds
 * internal information, the internal information also needs keys, though these
 * are never serialized.</li>
 * <li>Gathers information for the query as a whole, or for each segment within
 * a query. That is, it gathers information at the lowest-level where that
 * information is available.</li>
 * <li>Merges information back up the tree: from multiple segments, from multiple
 * servers, etc.</li>
 * <li>Manages headers size by dropping fields when the header would get too
 * large.</li>
 * </ul>
 *
 * A result is that the information the context may be incomplete if some of it
 * was previously dropped.
 *
 * <h4>API</h4>
 *
 * The query profile needs to obtain the full, untruncated information. To do this
 * it piggy-backs on the set operations to obtain the full value. To ensure this
 * is possible, code that works with standard values should call the <code>set</code>
 * functions provided which will do the needed map updated, and will set the value
 * in the query profile.
 * <p>
 * Extensions define additional keys. In this case, those marked as <code>TRAILER</code>
 * are added to the extensions area in the profile object.
 *
 * <h4>Query Profile</h4>
 *
 * This class holds a slice (sub-query) profile that contains fragments (a sub-query
 * which ran on a specific node) which contains operators. When merging fragment, each
 * becomes a separate entry within the slice, keyed by node. When running a sub-query,
 * a sub-query operator wraps the sub-query fragment.
 * <p>
 * This class also provides a way for one runner to returns its operator profile to
 * its parent runner. The <code>QueryRunner</code> is a functional: it has only one
 * method. Yet, we want that method to return both results and profile information.
 * To avoid making massive changes, we instead, use this class, which is passed to
 * each runner, to hold the operator profile returned to the parent. If the runner
 * produces no profile, then we substitute an "opaque" (empty) operator instead,
 * which allows us to change runners incrementally.
 */
@PublicApi
public abstract class ResponseContext
{
  /**
   * The base interface of a response context key.
   * Should be implemented by every context key.
   */
  public interface Key
  {
    @JsonValue
    String getName();

    /**
     * The phase (header, trailer, none) where this key is emitted.
     */
    Visibility getPhase();

    /**
     * Reads a value of this key from a JSON stream. Used by {@link ResponseContextDeserializer}.
     */
    Object readValue(JsonParser jp);

    /**
     * Merges two values of type T.
     *
     * This method may modify "oldValue" but must not modify "newValue".
     */
    Object mergeValues(Object oldValue, Object newValue);

    /**
     * Returns true if this key can be removed to reduce header size when the
     * header would otherwise be too large.
     */
    @JsonIgnore
    boolean canDrop();
  }

  /**
   * Where the key is emitted, if at all. Values in the context can be for internal
   * use only: for return before the query results (header) or only after query
   * results (trailer).
   */
  public enum Visibility
  {
    /**
     * Keys that are present in both the "X-Druid-Response-Context" header *and* the response context trailer.
     */
    HEADER_AND_TRAILER,

    /**
     * Keys that are only present in the response context trailer.
     */
    TRAILER,

    /**
     * Keys that are not present in query responses at all. Generally used for internal state tracking within a
     * single server.
     */
    NONE
  }

  /**
   * Abstract key class which provides most functionality except the
   * type-specific merge logic. Parsing is provided by an associated
   * parse function.
   */
  public abstract static class AbstractKey implements Key
  {
    private final String name;
    private final Visibility visibility;
    private final boolean canDrop;
    private final Function<JsonParser, Object> parseFunction;

    AbstractKey(String name, Visibility visibility, boolean canDrop, Class<?> serializedClass)
    {
      this.name = name;
      this.visibility = visibility;
      this.canDrop = canDrop;
      this.parseFunction = jp -> {
        try {
          return jp.readValueAs(serializedClass);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    }

    AbstractKey(String name, Visibility visibility, boolean canDrop, TypeReference<?> serializedTypeReference)
    {
      this.name = name;
      this.visibility = visibility;
      this.canDrop = canDrop;
      this.parseFunction = jp -> {
        try {
          return jp.readValueAs(serializedTypeReference);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public Visibility getPhase()
    {
      return visibility;
    }

    @Override
    public boolean canDrop()
    {
      return canDrop;
    }

    @Override
    public Object readValue(JsonParser jp)
    {
      return parseFunction.apply(jp);
    }

    @Override
    public String toString()
    {
      return name;
    }
  }

  /**
   * String valued attribute that holds the latest value assigned.
   */
  public static class StringKey extends AbstractKey
  {
    StringKey(String name, Visibility visibility, boolean canDrop)
    {
      super(name, visibility, canDrop, String.class);
    }

    @Override
    public Object mergeValues(Object oldValue, Object newValue)
    {
      return newValue;
    }
  }

  /**
   * Boolean valued attribute with the semantics that once the flag is
   * set true, it stays true.
   */
  public static class BooleanKey extends AbstractKey
  {
    BooleanKey(String name, Visibility visibility)
    {
      super(name, visibility, false, Boolean.class);
    }

    @Override
    public Object mergeValues(Object oldValue, Object newValue)
    {
      return (boolean) oldValue || (boolean) newValue;
    }
  }

  /**
   * Long valued attribute that holds the latest value assigned.
   */
  public static class LongKey extends AbstractKey
  {
    LongKey(String name, Visibility visibility)
    {
      super(name, visibility, false, Long.class);
    }

    @Override
    public Object mergeValues(Object oldValue, Object newValue)
    {
      return newValue;
    }
  }

  /**
   * Long valued attribute that holds the accumulation of values assigned.
   */
  public static class CounterKey extends AbstractKey
  {
    CounterKey(String name, Visibility visibility)
    {
      super(name, visibility, false, Long.class);
    }

    @Override
    public Object mergeValues(Object oldValue, Object newValue)
    {
      if (oldValue == null) {
        return newValue;
      }
      if (newValue == null) {
        return oldValue;
      }
      return (Long) oldValue + (Long) newValue;
    }
  }

  /**
   * Global registry of response context keys.
   * <p>
   * Also defines the standard keys associated with objects in the context.
   * <p>
   * If it's necessary to have some new keys in the context then they might be listed in a separate enum:
   * <pre>{@code
   * public class SomeClass
   * {
   *   static final Key EXTENSION_KEY_1 = new StringKey(
   *      "extension_key_1", Visibility.HEADER_AND_TRAILER, true),
   *   static final Key EXTENSION_KEY_2 = new CounterKey(
   *      "extension_key_2", Visibility.None);
   *
   *   static {
   *     Keys.instance().registerKeys(new Key[] {
   *        EXTENSION_KEY_1,
   *        EXTENSION_KEY_2
   *     });
   *   }
   * }</pre>
   * Make sure all extension keys are added with the {@link Keys#registerKey} or
   * {@link Keys#registerKeys} methods.
   * <p>
   * Predefined key types exist for common values. Custom values can be created as
   * shown below.
   */
  public static class Keys
  {
    /**
     * Lists intervals for which NO segment is present.
      */
    public static Key UNCOVERED_INTERVALS = new AbstractKey(
        "uncoveredIntervals",
        Visibility.HEADER_AND_TRAILER, true,
        new TypeReference<List<Interval>>()
        {
        })
    {
      @Override
      @SuppressWarnings("unchecked")
      public Object mergeValues(Object oldValue, Object newValue)
      {
        final List<Interval> result = new ArrayList<Interval>((List<Interval>) oldValue);
        result.addAll((List<Interval>) newValue);
        return result;
      }
    };

    /**
     * Indicates if the number of uncovered intervals exceeded the limit (true/false).
     */
    public static Key UNCOVERED_INTERVALS_OVERFLOWED = new BooleanKey(
        "uncoveredIntervalsOverflowed",
        Visibility.HEADER_AND_TRAILER);

    /**
     * Map of most relevant query ID to remaining number of responses from query nodes.
     * The value is initialized in {@code CachingClusteredClient} when it initializes the connection to the query nodes,
     * and is updated whenever they respond (@code DirectDruidClient). {@code RetryQueryRunner} uses this value to
     * check if the {@link #MISSING_SEGMENTS} is valid.
     *
     * Currently, the broker doesn't run subqueries in parallel, the remaining number of responses will be updated
     * one by one per subquery. However, since it can be parallelized to run subqueries simultaneously, we store them
     * in a ConcurrentHashMap.
     *
     * @see org.apache.druid.query.Query#getMostSpecificId
     */
    public static Key REMAINING_RESPONSES_FROM_QUERY_SERVERS = new AbstractKey(
        "remainingResponsesFromQueryServers",
        Visibility.NONE, true,
        Object.class)
    {
      @Override
      @SuppressWarnings("unchecked")
      public Object mergeValues(Object totalRemainingPerId, Object idAndNumResponses)
      {
        final ConcurrentHashMap<String, Integer> map = (ConcurrentHashMap<String, Integer>) totalRemainingPerId;
        final NonnullPair<String, Integer> pair = (NonnullPair<String, Integer>) idAndNumResponses;
        map.compute(
            pair.lhs,
            (id, remaining) -> remaining == null ? pair.rhs : remaining + pair.rhs);
        return map;
      }
    };

    /**
     * Lists missing segments.
     */
    public static Key MISSING_SEGMENTS = new AbstractKey(
        "missingSegments",
        Visibility.HEADER_AND_TRAILER, true,
        new TypeReference<List<SegmentDescriptor>>() {})
    {
      @Override
      @SuppressWarnings("unchecked")
      public Object mergeValues(Object oldValue, Object newValue)
      {
        final List<SegmentDescriptor> result = new ArrayList<SegmentDescriptor>((List<SegmentDescriptor>) oldValue);
        result.addAll((List<SegmentDescriptor>) newValue);
        return result;
      }
    };

    /**
     * Entity tag. A part of HTTP cache validation mechanism.
     * Is being removed from the context before sending and used as a separate HTTP header.
     */
    public static Key ETAG = new StringKey("ETag", Visibility.NONE, true);

    /**
     * Query total bytes gathered.
     */
    public static Key QUERY_TOTAL_BYTES_GATHERED = new AbstractKey(
        "queryTotalBytesGathered",
        Visibility.NONE, false,
        new TypeReference<AtomicLong>() {})
    {
      @Override
      public Object mergeValues(Object oldValue, Object newValue)
      {
        return ((AtomicLong) newValue).addAndGet(((AtomicLong) newValue).get());
      }
    };

    /**
     * This variable indicates when a running query should be expired,
     * and is effective only when 'timeout' of queryContext has a positive value.
     * Continuously updated by {@link org.apache.druid.query.scan.ScanQueryEngine}
     * by reducing its value on the time of every scan iteration.
     */
    public static Key TIMEOUT_AT = new LongKey(
        "timeoutAt",
        Visibility.NONE);

    /**
     * The number of rows scanned by {@link org.apache.druid.query.scan.ScanQueryEngine}.
     *
     * Named "count" for backwards compatibility with older data servers that still send this, even though it's now
     * marked with {@link Visibility#NONE}.
     */
    public static Key NUM_SCANNED_ROWS = new CounterKey(
        "count",
        Visibility.NONE);

    /**
     * The total CPU time for threads related to Sequence processing of the query.
     * Resulting value on a Broker is a sum of downstream values from historicals / realtime nodes.
     * For additional information see {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
     */
    public static Key CPU_CONSUMED_NANOS = new CounterKey(
        "cpuConsumed",
        Visibility.TRAILER);

    /**
     * Indicates if a {@link ResponseContext} was truncated during serialization.
     */
    public static Key TRUNCATED = new BooleanKey(
        "truncated",
        Visibility.HEADER_AND_TRAILER);

    /**
     * TODO(gianm): Javadocs.
     */
    public static Key METRICS = new AbstractKey(
        "metrics",
        Visibility.TRAILER, false,
        new TypeReference<MultiQueryMetricsCollector>() {}
    ) {
      @Override
      public Object mergeValues(Object oldValue, Object newValue)
      {
        final MultiQueryMetricsCollector currentCollector = (MultiQueryMetricsCollector) oldValue;
        return currentCollector.addAll((MultiQueryMetricsCollector) newValue);
      }
    };

    public static Key PROFILE = new AbstractKey(
        "profile",
        Visibility.TRAILER, false,
        new TypeReference<Map<String, Object>>() {}
    ) {
      @Override
      public Object mergeValues(Object oldValue, Object newValue)
      {
        return newValue;
      }
    };

    /**
     * One and only global list of keys. This is a semi-constant: it is mutable
     * at start-up time, but then is not thread-safe, and must remain unchanged
     * for the duration of the server run.
     */
    public static final Keys INSTANCE = new Keys();

    /**
     * ConcurrentSkipListMap is used to have the natural ordering of its keys.
     * Thread-safe structure is required since there is no guarantee that {@link #registerKey(BaseKey)}
     * would be called only from class static blocks.
     */
    private ConcurrentMap<String, Key> registered_keys = new ConcurrentSkipListMap<>();

    static {
      instance().registerKeys(new Key[]
      {
          UNCOVERED_INTERVALS,
          UNCOVERED_INTERVALS_OVERFLOWED,
          REMAINING_RESPONSES_FROM_QUERY_SERVERS,
          MISSING_SEGMENTS,
          ETAG,
          QUERY_TOTAL_BYTES_GATHERED,
          TIMEOUT_AT,
          NUM_SCANNED_ROWS,
          CPU_CONSUMED_NANOS,
          TRUNCATED,
          METRICS,
          PROFILE,
      });
    }

    /**
     * Returns the single, global key registry for this server.
     */
    public static Keys instance()
    {
      return INSTANCE;
    }

    /**
     * Primary way of registering context keys.
     *
     * @throws IllegalArgumentException if the key has already been registered.
     */
    public void registerKey(Key key)
    {
      if (registered_keys.putIfAbsent(key.getName(), key) != null) {
        throw new IAE("Key [%s] has already been registered as a context key", key.getName());
      }
    }

    /**
     * Register a group of keys.
     */
    public void registerKeys(Key[] keys)
    {
      for (Key key : keys) {
        registerKey(key);
      }
    }

    /**
     * Returns a registered key associated with the name {@param name}.
     *
     * @throws IllegalStateException if a corresponding key has not been registered.
     */
    public Key keyOf(String name)
    {
      Key key = registered_keys.get(name);
      if (key == null) {
        throw new ISE("Key [%s] is not registered as a context key", name);
      }
      return key;
    }

    /**
     * Returns all keys registered via {@link Key#registerKey}.
     */
    public Collection<Key> getAllRegisteredKeys()
    {
      return Collections.unmodifiableCollection(registered_keys.values());
    }
  }

  private final List<List<OperatorProfile>> profileStack;

  public ResponseContext()
  {
    profileStack = new ArrayList<>();
    profileStack.add(new ArrayList<>());
  }

  protected abstract Map<Key, Object> getDelegate();

  public Map<String, Object> toMap()
  {
    return getDelegate().entrySet()
      .stream()
      .collect(
          Collectors.toMap(
              e -> e.getKey().getName(),
              Map.Entry::getValue
          )
      );
  }

  private static final Comparator<Map.Entry<String, JsonNode>> VALUE_LENGTH_REVERSED_COMPARATOR =
      Comparator.comparing((Map.Entry<String, JsonNode> e) -> e.getValue().toString().length()).reversed();

  /**
   * Create an empty DefaultResponseContext instance
   *
   * @return empty DefaultResponseContext instance
   */
  public static ResponseContext createEmpty()
  {
    return DefaultResponseContext.createEmpty();
  }

  /**
   * Initialize fields for a query context. Not needed when merging.
   */
  public void initialize()
  {
    putValue(Keys.QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());
    initializeRemainingResponses();
  }

  public void initializeRemainingResponses()
  {
    putValue(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new ConcurrentHashMap<>());
  }

  public void initializeMissingSegments()
  {
    putValue(Keys.MISSING_SEGMENTS, new ArrayList<>());
  }

  public void initializeRowScanCount()
  {
    putValue(Keys.NUM_SCANNED_ROWS, 0L);
  }

  /**
   * Deserializes a string into {@link ResponseContext} using given {@link ObjectMapper}.
   *
   * @throws IllegalStateException if one of the deserialized map keys has not been registered.
   */
  public static ResponseContext deserialize(String responseContext, ObjectMapper objectMapper) throws IOException
  {
    // TODO(gianm): Remove this method
    return objectMapper.readValue(responseContext, ResponseContext.class);
  }

  /**
   * Associates the specified object with the specified extension key.
   *
   * @throws IllegalStateException if the key has not been registered.
   */
  public Object put(Key key, Object value)
  {
    final Key registeredKey = Keys.instance().keyOf(key.getName());
    return putValue(registeredKey, value);
  }

  public void putUncoveredIntervals(List<Interval> intervals, boolean overflowed)
  {
    putValue(Keys.UNCOVERED_INTERVALS, intervals);
    putValue(Keys.UNCOVERED_INTERVALS_OVERFLOWED, overflowed);
  }

  public void putEntityTag(String eTag)
  {
    putValue(Keys.ETAG, eTag);
  }

  public void putTimeoutTime(long time)
  {
    putValue(Keys.TIMEOUT_AT, time);
  }

  /**
   * Associates the specified object with the specified key. Assumes that
   * the key is validated.
   */
  private Object putValue(Key key, Object value)
  {
    return getDelegate().put(key, value);
  }

  public Object get(Key key)
  {
    return getDelegate().get(key);
  }

  @SuppressWarnings("unchecked")
  public ConcurrentHashMap<String, Integer> getRemainingResponses()
  {
    return (ConcurrentHashMap<String, Integer>) get(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS);
  }

  @SuppressWarnings("unchecked")
  public List<Interval> getUncoveredIntervals()
  {
    return (List<Interval>) get(Keys.UNCOVERED_INTERVALS);
  }

  @SuppressWarnings("unchecked")
  public List<SegmentDescriptor> getMissingSegments()
  {
    return (List<SegmentDescriptor>) get(Keys.MISSING_SEGMENTS);
  }

  public boolean getTruncated()
  {
    Boolean value = (Boolean) get(Keys.TRUNCATED);
    return value == null ? false : value;
  }

  public String getEntityTag()
  {
    return (String) get(Keys.ETAG);
  }

  public AtomicLong getTotalBytes()
  {
    return (AtomicLong) get(Keys.QUERY_TOTAL_BYTES_GATHERED);
  }

  public Long getTimeoutTime()
  {
    return (Long) get(Keys.TIMEOUT_AT);
  }

  public Long getRowScanCount()
  {
    return (Long) get(Keys.NUM_SCANNED_ROWS);
  }

  public Long getCpuNanos()
  {
    return (Long) get(Keys.CPU_CONSUMED_NANOS);
  }

  public Object remove(Key key)
  {
    return getDelegate().remove(key);
  }

  /**
   * Adds (merges) a new value associated with a key to an old value.
   * See merge function of a context key for a specific implementation.
   *
   * @throws IllegalStateException if the key has not been registered.
   */
  public Object add(Key key, Object value)
  {
    final Key registeredKey = Keys.instance().keyOf(key.getName());
    return addValue(registeredKey, value);
  }

  public void addRemainingResponse(String id, int count)
  {
    addValue(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS,
        new NonnullPair<>(id, count));
  }

  public void addMissingSegments(List<SegmentDescriptor> descriptors)
  {
    addValue(Keys.MISSING_SEGMENTS, descriptors);
  }

  public void addRowScanCount(long count)
  {
    addValue(Keys.NUM_SCANNED_ROWS, count);
  }

  public void addCpuNanos(long ns)
  {
    addValue(Keys.CPU_CONSUMED_NANOS, ns);
  }

  private Object addValue(Key key, Object value)
  {
    return getDelegate().merge(key, value, key::mergeValues);
  }

  /**
   * Merges a response context into the current.
   *
   * @throws IllegalStateException If a key of the {@code responseContext} has not been registered.
   */
  public void merge(ResponseContext responseContext)
  {
    responseContext.getDelegate().forEach((key, newValue) -> {
      if (newValue != null) {
        add(key, newValue);
      }
    });
  }

  /**
   * Serializes the context given that the resulting string length is less than the provided limit.
   * This method removes some elements from context collections if it's needed to satisfy the limit.
   * There is no explicit priorities of keys which values are being truncated.
   * Any kind of key can be removed, the key's <code>canDrop()</code> attribute indicates
   * which can be dropped. (The unit tests use a string key.)
   * Thus keys as equally prioritized and starts removing elements from
   * the array which serialized value length is the biggest.
   * The resulting string will be correctly deserialized to {@link ResponseContext}.
   */
  public SerializationResult toHeader(ObjectMapper objectMapper, int maxCharsNumber)
      throws JsonProcessingException
  {
    final Map<Key, Object> headerMap =
        getDelegate().entrySet()
                     .stream()
                     .filter(entry -> entry.getKey().getPhase() == Visibility.HEADER_AND_TRAILER)
                     .collect(
                         Collectors.toMap(
                             Map.Entry::getKey,
                             Map.Entry::getValue
                         )
                     );

    final String fullSerializedString = objectMapper.writeValueAsString(headerMap);
    if (fullSerializedString.length() <= maxCharsNumber) {
      return new SerializationResult(null, fullSerializedString);
    }

    int needToRemoveCharsNumber = fullSerializedString.length() - maxCharsNumber;
    // Indicates that the context is truncated during serialization.
    headerMap.put(Keys.TRUNCATED, true);
    // Account for the extra field just added
    needToRemoveCharsNumber += Keys.TRUNCATED.getName().length() + 7;
    final ObjectNode contextJsonNode = objectMapper.valueToTree(headerMap);
    final List<Map.Entry<String, JsonNode>> sortedNodesByLength = Lists.newArrayList(contextJsonNode.fields());
    sortedNodesByLength.sort(VALUE_LENGTH_REVERSED_COMPARATOR);
    // The complexity of this block is O(n*m*log(m)) where n - context size, m - context's array size
    for (Map.Entry<String, JsonNode> e : sortedNodesByLength) {
      final String fieldName = e.getKey();
      if (!Keys.instance().keyOf(fieldName).canDrop()) {
        continue;
      }
      final JsonNode node = e.getValue();
      int removeLength = fieldName.toString().length() + node.toString().length();
      if (removeLength < needToRemoveCharsNumber || !(node instanceof ArrayNode)) {
        // Remove the field
        contextJsonNode.remove(fieldName);
        needToRemoveCharsNumber -= removeLength;
      } else {
        final ArrayNode arrayNode = (ArrayNode) node;
        int removed = removeNodeElementsToSatisfyCharsLimit(arrayNode, needToRemoveCharsNumber);
        if (arrayNode.size() == 0) {
          // The field is now empty, removing it because an empty array field may be misleading
          // for the recipients of the truncated response context.
          contextJsonNode.remove(fieldName);
          needToRemoveCharsNumber -= removeLength;
        } else {
          needToRemoveCharsNumber -= removed;
        }
      }

      if (needToRemoveCharsNumber <= 0) {
        break;
      }
    }

    if (needToRemoveCharsNumber > 0) {
      // Still too long, and no more shortenable keys.
      throw new ISE("Response context of length %d is too long for header, max = %d; minimum size = %d",
              fullSerializedString.length(), maxCharsNumber, maxCharsNumber - needToRemoveCharsNumber);
    }

    return new SerializationResult(contextJsonNode.toString(), fullSerializedString);
  }

  /**
   * Return a map containing the key/value pairs to be returned in the
   * response trailer. Note that, unlike the header, the trailer is optional
   * and does not have a size limit.
   */
  public Map<Key, Object> trailerCopy()
  {
    return getDelegate().entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().getPhase() != Visibility.NONE)
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                            )
                        );
  }

  /**
   * Removes {@code node}'s elements which total length of serialized values is greater or equal to the passed limit.
   * If it is impossible to satisfy the limit the method removes all {@code node}'s elements.
   * On every iteration it removes exactly half of the remained elements to reduce the overall complexity.
   *
   * @param node   {@link ArrayNode} which elements are being removed.
   * @param target the number of chars need to be removed.
   *
   * @return the number of removed chars.
   */
  private static int removeNodeElementsToSatisfyCharsLimit(ArrayNode node, int target)
  {
    int nodeLen = node.toString().length();
    final int startLen = nodeLen;
    while (node.size() > 0 && target > startLen - nodeLen) {
      // Reducing complexity by removing half of array's elements
      final int removeUntil = node.size() / 2;
      for (int removeAt = node.size() - 1; removeAt >= removeUntil; removeAt--) {
        node.remove(removeAt);
      }
      nodeLen = node.toString().length();
    }
    return startLen - nodeLen;
  }

  /**
   * Serialization result of {@link ResponseContext}.
   * Response context might be serialized using max length limit, in this case the context might be reduced
   * by removing max-length fields one by one unless serialization result length is less than the limit.
   * This structure has a reduced serialization result along with full result and boolean property
   * indicating if some fields were removed from the context.
   */
  public static class SerializationResult
  {
    @Nullable
    private final String truncatedResult;
    private final String fullResult;

    SerializationResult(@Nullable String truncatedResult, String fullResult)
    {
      this.truncatedResult = truncatedResult;
      this.fullResult = fullResult;
    }

    /**
     * Returns the truncated result if it exists otherwise returns the full result.
     */
    public String getResult()
    {
      return isTruncated() ? truncatedResult : fullResult;
    }

    public String getFullResult()
    {
      return fullResult;
    }

    public boolean isTruncated()
    {
      return truncatedResult != null;
    }
  }

  public void pushGroup()
  {
    profileStack.add(new ArrayList<>());
  }

  public List<OperatorProfile> popGroup()
  {
    if (profileStack.size() < 2) {
      throw new ISE("Profile group stack underflow");
    }
    return profileStack.remove(profileStack.size() - 1);
  }

  public void pushProfile(OperatorProfile profile)
  {
    profileStack.get(profileStack.size() - 1).add(profile);
  }

  public OperatorProfile popProfile()
  {
    List<OperatorProfile> tail = profileStack.get(profileStack.size() - 1);
    if (tail.isEmpty()) {
      return new OpaqueOperator();
    }
    return tail.remove(tail.size() - 1);
  }
}
