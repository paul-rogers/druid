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

package org.apache.druid.query.profile;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Useful instrumented suppliers.
 *
 * <p>This is a variation of the Guava Suppliers concept that takes an
 * instrumentation parameter that records metrics for the operation. This is a direct
 * copy of the code there, with the addition of instrumentation. However, this
 * version is not serializable as that feature is not needed for Druid.
 * <p>
 * In memoized form, the metrics an extension of the {@code Metrics} interface.
 * If the item is in the cache (is already memoized), then {@code gotten()} is called
 * with {@code true} to record the cache hit. Else, if the item is not memoized,
 * {@code gotten()} is called with {@code false}, <i>and</i> the metrics are passed
 * into the delegate to record the cost of materializing the item.
 *
 * @see {@ com.google.common.base.Suppliers}
 */
public interface InstrumentedSupplier<T, M>
{
  public interface Metrics {
    void gotten(boolean cacheHit);
  }

  public static class MetricsStub implements Metrics
  {
    @Override
    public void gotten(boolean cacheHit)
    {
    }
  }

  T get(M metrics);

  /**
   * Returns a supplier which caches the instance retrieved during the first
   * call to {@code get()} and returns that value on subsequent calls to
   * {@code get()}. See:
   * <a href="http://en.wikipedia.org/wiki/Memoization">memoization</a>
   *
   * <p>The returned supplier is thread-safe. The supplier's serialized form
   * does not contain the cached value, which will be recalculated when {@code
   * get()} is called on the reserialized instance.
   *
   * <p>If {@code delegate} is an instance created by an earlier call to {@code
   * memoize}, it is returned directly.
   */
  public static <T, M extends Metrics> InstrumentedSupplier<T, M> memoize(InstrumentedSupplier<T, M> delegate)
  {
    return (delegate instanceof MemoizingSupplier)
        ? delegate
        : new MemoizingSupplier<T, M>(Preconditions.checkNotNull(delegate));
  }

  @VisibleForTesting
  static class MemoizingSupplier<T, M extends Metrics> implements InstrumentedSupplier<T, M>
  {
    final InstrumentedSupplier<T, M> delegate;
    transient volatile boolean initialized;
    // "value" does not need to be volatile; visibility piggy-backs
    // on volatile read of "initialized".
    transient T value;

    MemoizingSupplier(InstrumentedSupplier<T, M> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public T get(M metrics)
    {
      boolean hit = true;
      // A 2-field variant of Double Checked Locking.
      if (!initialized) {
        synchronized (this) {
          if (!initialized) {
            hit = false;
            T t = delegate.get(metrics);
            value = t;
            initialized = true;
            return t;
          }
        }
      }
      metrics.gotten(hit);
      return value;
    }

    @Override public String toString() {
      return "InstrumentedSupplier.memoize(" + delegate + ")";
    }
  }

  /**
   * Opposite of the memoized form: this form indicates that the supplied item is
   * "permanently cached": materialized ahead of time and stored. Allows this case
   * to work the same as the memoized case for metrics gathering: all accesses count
   * as a cache hit. The cost of load is counted separately from access, and is
   * attributed to whatever caused the original load to occur.
   */
  public static <T, M extends Metrics> InstrumentedSupplier<T, M> cached(InstrumentedSupplier<T, M> delegate)
  {
    return new InstrumentedSupplier<T, M>()
    {
      @Override
      public T get(M metrics)
      {
        metrics.gotten(true);
        return delegate.get(metrics);
      }
    };
  }
}
