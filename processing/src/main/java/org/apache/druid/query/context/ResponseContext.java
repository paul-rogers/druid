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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.MultiQueryMetricsCollector;
import org.apache.druid.query.SegmentDescriptor;
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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The context for storing and passing data between chains of {@link org.apache.druid.query.QueryRunner}s.
 * The context is also transferred between Druid nodes with all the data it contains.
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
      return (long) oldValue + ((Number) newValue).longValue();
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
        new TypeReference<List<SegmentDescriptor>>()
        {
        })
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
    public static Key QUERY_TOTAL_BYTES_GATHERED = new LongKey(
        "queryTotalBytesGathered",
        Visibility.NONE);
    
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
          METRICS
      });
    }
    
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
        throw new ISE("Key [%s] has not yet been registered as a context key", name);
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

  protected abstract Map<Key, Object> getDelegate();

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
   * Associates the specified object with the specified key.
   *
   * @throws IllegalStateException if the key has not been registered.
   */
  public Object put(Key key, Object value)
  {
    final Key registeredKey = Keys.instance().keyOf(key.getName());
    return getDelegate().put(registeredKey, value);
  }

  public Object get(Key key)
  {
    return getDelegate().get(key);
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
    return getDelegate().merge(registeredKey, value, key::mergeValues);
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
      throw new ISE(
          StringUtils.format(
              "Response context of length %d is too long for header, max = %d; minimum size = %d",
              fullSerializedString.length(), maxCharsNumber, maxCharsNumber - needToRemoveCharsNumber));
    }

    return new SerializationResult(contextJsonNode.toString(), fullSerializedString);
  }

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
   * @param node                    {@link ArrayNode} which elements are being removed.
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
}
