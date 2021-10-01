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
  public interface BaseKey
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

  /**
   * Keys associated with objects in the context.
   * <p>
   * If it's necessary to have some new keys in the context then they might be listed in a separate enum:
   * <pre>{@code
   * public enum ExtensionResponseContextKey implements BaseKey
   * {
   *   EXTENSION_KEY_1("extension_key_1"),
   *   EXTENSION_KEY_2("extension_key_2");
   *
   *   static {
   *     for (BaseKey key : values()) ResponseContext.Key.registerKey(key);
   *   }
   *
   *   private final String name;
   *   private final BiFunction<Object, Object, Object> mergeFunction;
   *
   *   ExtensionResponseContextKey(String name)
   *   {
   *     this.name = name;
   *     this.mergeFunction = (oldValue, newValue) -> newValue;
   *   }
   *
   *   @Override public String getName() { return name; }
   *
   *   @Override public BiFunction<Object, Object, Object> getMergeFunction() { return mergeFunction; }
   * }
   * }</pre>
   * Make sure all extension enum values added with {@link Key#registerKey} method.
   */
  public enum Key implements BaseKey
  {
    /**
     * Lists intervals for which NO segment is present.
     */
    UNCOVERED_INTERVALS(
    		"uncoveredIntervals", 
    		Visibility.HEADER_AND_TRAILER, true,
    		new TypeReference<List<Interval>>() {}) {
      @Override
      @SuppressWarnings("unchecked")
      public Object mergeValues(Object oldValue, Object newValue)
      {
        final ArrayList<Interval> result = new ArrayList<Interval>((List<Interval>) oldValue);
        result.addAll((List<Interval>) newValue);
        return result;
      }
    },
    /**
     * Indicates if the number of uncovered intervals exceeded the limit (true/false).
     */
    UNCOVERED_INTERVALS_OVERFLOWED(
    		"uncoveredIntervalsOverflowed", 
    		Visibility.HEADER_AND_TRAILER, false,
    		Boolean.class) {
      @Override
      public Object mergeValues(Object oldValue, Object newValue)
      {
        return (boolean) oldValue || (boolean) newValue;
      }
    },
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
    REMAINING_RESPONSES_FROM_QUERY_SERVERS(
    		"remainingResponsesFromQueryServers", 
    		Visibility.NONE, true,
    		Object.class) {
      @Override
      @SuppressWarnings("unchecked")
      public Object mergeValues(Object totalRemainingPerId, Object idAndNumResponses)
      {
        final ConcurrentHashMap<String, Integer> map = (ConcurrentHashMap<String, Integer>) totalRemainingPerId;
        final NonnullPair<String, Integer> pair = (NonnullPair<String, Integer>) idAndNumResponses;
        map.compute(
            pair.lhs,
            (id, remaining) -> remaining == null ? pair.rhs : remaining + pair.rhs
        );
        return map;
      }
    },
    /**
     * Lists missing segments.
     */
    MISSING_SEGMENTS(
        "missingSegments",
        Visibility.HEADER_AND_TRAILER, true,
        new TypeReference<List<SegmentDescriptor>>() {}
    ) {
      @Override
      @SuppressWarnings("unchecked")
      public Object mergeValues(Object oldValue, Object newValue)
      {
        final List<SegmentDescriptor> result = new ArrayList<SegmentDescriptor>((List<SegmentDescriptor>) oldValue);
        result.addAll((List<SegmentDescriptor>) newValue);
        return result;
      }
    },
    /**
     * Entity tag. A part of HTTP cache validation mechanism.
     * Is being removed from the context before sending and used as a separate HTTP header.
     */
    ETAG("ETag", Visibility.NONE, true, String.class),
    /**
     * Query total bytes gathered.
     */
    QUERY_TOTAL_BYTES_GATHERED(
    		"queryTotalBytesGathered", 
    		Visibility.NONE, true,
    		Long.class),
    /**
     * This variable indicates when a running query should be expired,
     * and is effective only when 'timeout' of queryContext has a positive value.
     * Continuously updated by {@link org.apache.druid.query.scan.ScanQueryEngine}
     * by reducing its value on the time of every scan iteration.
     */
    TIMEOUT_AT(
    		"timeoutAt", 
    		Visibility.NONE, true,
    		Long.class),
    /**
     * The number of rows scanned by {@link org.apache.druid.query.scan.ScanQueryEngine}.
     *
     * Named "count" for backwards compatibility with older data servers that still send this, even though it's now
     * marked with {@link Visibility#NONE}.
     */
    NUM_SCANNED_ROWS("count", 
    		Visibility.NONE, true,
    		Long.class) {
      @Override
      public Object mergeValues(Object oldValue, Object newValue)
      {
        return (long) oldValue + ((Number) newValue).longValue();
      }
    },
    /**
     * The total CPU time for threads related to Sequence processing of the query.
     * Resulting value on a Broker is a sum of downstream values from historicals / realtime nodes.
     * For additional information see {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
     */
    CPU_CONSUMED_NANOS(
    		"cpuConsumed", 
    		Visibility.TRAILER, false,
    		Long.class) {
      @Override
      public Object mergeValues(Object oldValue, Object newValue)
      {
        return (long) oldValue + ((Number) newValue).longValue();
      }
    },
    /**
     * Indicates if a {@link ResponseContext} was truncated during serialization.
     */
    TRUNCATED(
    		"truncated", 
    		Visibility.HEADER_AND_TRAILER, false,
    		Boolean.class) {
      @Override
      public Object mergeValues(Object oldValue, Object newValue)
      {
        return (boolean) oldValue || (boolean) newValue;
      }
    },
    /**
     * TODO(gianm): Javadocs.
     */
    METRICS(
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

    /**
     * ConcurrentSkipListMap is used to have the natural ordering of its keys.
     * Thread-safe structure is required since there is no guarantee that {@link #registerKey(BaseKey)}
     * would be called only from class static blocks.
     */
    private static final ConcurrentMap<String, BaseKey> REGISTERED_KEYS = new ConcurrentSkipListMap<>();

    static {
      for (BaseKey key : values()) {
        registerKey(key);
      }
    }

    /**
     * Primary way of registering context keys.
     *
     * @throws IllegalArgumentException if the key has already been registered.
     */
    public static void registerKey(BaseKey key)
    {
      if (REGISTERED_KEYS.putIfAbsent(key.getName(), key) != null) {
        throw new IAE("Key [%s] has already been registered as a context key", key.getName());
      }
    }

    /**
     * Returns a registered key associated with the name {@param name}.
     *
     * @throws IllegalStateException if a corresponding key has not been registered.
     */
    public static BaseKey keyOf(String name)
    {
      BaseKey key = REGISTERED_KEYS.get(name);
      if (key == null) {
        throw new ISE("Key [%s] has not yet been registered as a context key", name);
      }
      return key;
    }

    /**
     * Returns all keys registered via {@link Key#registerKey}.
     */
    public static Collection<BaseKey> getAllRegisteredKeys()
    {
      return Collections.unmodifiableCollection(REGISTERED_KEYS.values());
    }

    private final String name;
    private final Visibility visibility;
    private final boolean canDrop;
    private final Function<JsonParser, Object> parseFunction;

    Key(String name, Visibility visibility, boolean canDrop, Class<?> serializedClass)
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

    Key(String name, Visibility visibility, boolean canDrop, TypeReference<?> serializedTypeReference)
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
    public Object mergeValues(Object oldValue, Object newValue)
    {
      return newValue;
    }
  }

  protected abstract Map<BaseKey, Object> getDelegate();

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
  public Object put(BaseKey key, Object value)
  {
    final BaseKey registeredKey = Key.keyOf(key.getName());
    return getDelegate().put(registeredKey, value);
  }

  public Object get(BaseKey key)
  {
    return getDelegate().get(key);
  }

  public Object remove(BaseKey key)
  {
    return getDelegate().remove(key);
  }

  /**
   * Adds (merges) a new value associated with a key to an old value.
   * See merge function of a context key for a specific implementation.
   *
   * @throws IllegalStateException if the key has not been registered.
   */
  public Object add(BaseKey key, Object value)
  {
    final BaseKey registeredKey = Key.keyOf(key.getName());
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
   * There is no explicit priorities of keys which values are being truncated because for now there are only
   * two potential limit breaking keys ({@link Key#UNCOVERED_INTERVALS}
   * and {@link Key#MISSING_SEGMENTS}) and their values are arrays.
   * Thus current implementation considers these arrays as equal prioritized and starts removing elements from
   * the array which serialized value length is the biggest.
   * The resulting string might be correctly deserialized to {@link ResponseContext}.
   */
  public SerializationResult toHeader(ObjectMapper objectMapper, int maxCharsNumber)
      throws JsonProcessingException
  {
    final Map<BaseKey, Object> headerMap =
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
    headerMap.put(Key.TRUNCATED, true);
    // Account for the extra field just added
    needToRemoveCharsNumber += Key.TRUNCATED.getName().length() + 7;
    final ObjectNode contextJsonNode = objectMapper.valueToTree(headerMap);
    final List<Map.Entry<String, JsonNode>> sortedNodesByLength = Lists.newArrayList(contextJsonNode.fields());
    sortedNodesByLength.sort(VALUE_LENGTH_REVERSED_COMPARATOR);
    // The complexity of this block is O(n*m*log(m)) where n - context size, m - context's array size
    for (Map.Entry<String, JsonNode> e : sortedNodesByLength) {
      final String fieldName = e.getKey();
      final JsonNode node = e.getValue();
      if (Key.keyOf(fieldName).canDrop()) {
        int removeLength = node.toString().length() + node.asText().length();
        if (node instanceof ArrayNode) {
          final ArrayNode arrayNode = (ArrayNode) node;
          int oldTarget = needToRemoveCharsNumber;
          needToRemoveCharsNumber -= removeNodeElementsToSatisfyCharsLimit(arrayNode, needToRemoveCharsNumber);
          if (arrayNode.size() == 0) {
            // The field is now empty, removing it because an empty array field may be misleading
            // for the recipients of the truncated response context.
            contextJsonNode.remove(fieldName);
            needToRemoveCharsNumber = oldTarget - removeLength;
          }
        } else {
          // Remove the field
          contextJsonNode.remove(fieldName);
          needToRemoveCharsNumber -= removeLength;
        }
      }

      if (needToRemoveCharsNumber <= 0) {
        break;
      }
    }

    if (needToRemoveCharsNumber > 0) {
      // Still too long, and no more shortenable keys.
      throw new ISE("Response context too long for header; cannot shorten");
    }

    return new SerializationResult(contextJsonNode.toString(), fullSerializedString);
  }

  public Map<BaseKey, Object> trailerCopy()
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
   * @param needToRemoveCharsNumber the number of chars need to be removed.
   *
   * @return the number of removed chars.
   */
  private static int removeNodeElementsToSatisfyCharsLimit(ArrayNode node, int needToRemoveCharsNumber)
  {
    int removedCharsNumber = 0;
    while (node.size() > 0 && needToRemoveCharsNumber > removedCharsNumber) {
      final int lengthBeforeRemove = node.toString().length();
      // Reducing complexity by removing half of array's elements
      final int removeUntil = node.size() / 2;
      for (int removeAt = node.size() - 1; removeAt >= removeUntil; removeAt--) {
        node.remove(removeAt);
      }
      final int lengthAfterRemove = node.toString().length();
      removedCharsNumber += lengthBeforeRemove - lengthAfterRemove;
    }
    return removedCharsNumber;
  }

  /**
   * Serialization result of {@link ResponseContext}.
   * Response context might be serialized using max legth limit, in this case the context might be reduced
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
