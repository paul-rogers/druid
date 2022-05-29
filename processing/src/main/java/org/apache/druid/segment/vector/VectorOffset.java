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

package org.apache.druid.segment.vector;

/**
 * The movable version of ReadableVectorOffset.
 * <p>
 * Protocol is:
 * <ul>
 * <li>Check {@link #isDone()}.</li>
 * <li>If {@code false}, read a vector of length {@link #getCurrentVectorSize()}.</li>
 * <li>Call {@link #advance()}.</li>
 * <li>Repeat.</li>
 * </ul>
 * That is: <pre><code>
 * while (!offset.isDone()) {
 *   // Do something with a vector of size offset.getCurrentVectorSize()
 *   offset.advance()
 * }</code></pre>
 * <p>
 * Note that the value of {@link #getStartOffset()} and
 * {@link #getCurrentVectorSize()} are undefined after
 * {@link #isDone()} returns {@code false}.
 *
 * @see org.apache.druid.segment.data.Offset, the non-vectorized version.
 */
public interface VectorOffset extends ReadableVectorOffset
{
  /**
   * Advances by one batch.
   */
  void advance();

  /**
   * Checks if iteration is "done", meaning the current batch of offsets is empty, and there are no more coming.
   */
  boolean isDone();

  /**
   * Resets the object back to its original state. Once this is done, iteration can begin anew.
   */
  void reset();
}
