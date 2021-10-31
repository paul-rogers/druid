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

package org.apache.druid.server.pipeline;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.druid.client.cache.Cache;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.pipeline.Operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

/**
 * Operator to retrieve a query value from the cache or child. If the results
 * are present in the cache, {@code open()} returns an iterator over those
 * results.
 * If not present, returns the iterator for the child operator,
 * which presumably will produce the result.
 */
public class FromCacheOperator implements Operator
{
  enum State
  {
    START, FROM_CACHE, RUN, CLOSED
  }
  private final Operator child;
  private final CacheStrategy<?, ?, ?> strategy;
  private final Cache cache;
  private final Cache.NamedKey key;
  private final ObjectMapper mapper;
  private State state = State.START;

  public FromCacheOperator(
      final CacheStrategy<?, ?, ?> strategy,
      final Cache cache,
      final Cache.NamedKey key,
      final ObjectMapper mapper,
      final Operator child
      )
  {
    this.strategy = strategy;
    this.cache = cache;
    this.key = key;
    this.mapper = mapper;
    this.child = child;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    Preconditions.checkState(state == State.START);
    final byte[] cachedResult = cache.get(key);
    if (cachedResult == null) {
      // No cached value, child will produce "live" value.
      state = State.RUN;
      return child.open(context);
    }

    // Cached value
    state = State.FROM_CACHE;
    if (cachedResult.length == 0) {
      // But the result is empty
      return Collections.emptyIterator();
    }

    // Else, return the cached result as an iterator
    try {
      return (Iterator<Object>) mapper.readValues(
          mapper.getFactory().createParser(cachedResult),
          strategy.getCacheObjectClazz()
      );
    }
    catch (IOException e) {
      state = State.CLOSED;
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close(boolean cascade)
  {
    if (state == State.RUN && cascade) {
      child.close(cascade);
    }
    state = State.CLOSED;
  }
}
