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

import java.util.Iterator;

import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.pipeline.Operator;
import org.apache.druid.query.pipeline.Operators;
import org.apache.druid.query.pipeline.SequenceOperator;

import com.google.common.base.Function;

public class ToCacheOperator implements Operator
{
  private final Operator child;
  private final Cache cache;
  private final CachePopulator cachePopulator;
  private final Cache.NamedKey key;
  private final CacheStrategy<?, ?, ?> strategy;

  public ToCacheOperator(
      final CacheStrategy<?, ?, ?> strategy,
      final Cache cache,
      final Cache.NamedKey key,
      final CachePopulator cachePopulator,
      final Operator child
  )
  {
    this.strategy = strategy;
    this.cache = cache;
    this.key = key;
    this.cachePopulator = cachePopulator;
    this.child = child;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    // Round-about way to cache results. The populator wants a sequence.
    // Thus, we have to wrap the child in a sequence, hand it to the populator,
    // which wraps it again, then we wrap it again to get back an iterator.
    //
    // TODO: modify the populator to look like a consumer: open, write, close.
    //
    // Further, we really don't care about the type of the objects, so we're
    // fighting the type parameters: doesn't matter what the type is, as long
    // as the cache is producing what the downstream operators want.
    final Function cacheFn = strategy.pullFromSegmentLevelCache();
    final Sequence<?> input = Operators.toSequence(child, context);
    final Sequence<?> output = cachePopulator.wrap(input, value -> cacheFn.apply(value), cache, key);
    return new SequenceOperator(output).open(context);
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      child.close(cascade);
    }
  }

}
