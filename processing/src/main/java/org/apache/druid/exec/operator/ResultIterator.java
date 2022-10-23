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

package org.apache.druid.exec.operator;

/**
 * Iterator over operator results. Operators do not use the Java
 * {@code Iterator} class: the simpler implementation here
 * minimizes per-row overhead. An {@code OperatorIterator} can
 * be converted to a Java {@code Iterator} by calling
 * {@link static <T> Iterator<T> Operators#toIterator(Operator<T>)},
 * but that approach adds overhead.
 */
public interface ResultIterator
{
  public class StallException extends Exception
  {
  }

  /**
   * Exception thrown at EOF.
   */
  public class EofException extends StallException
  {
  }

  /**
   * Indicates a pause in async execution. The operator can run, but
   * is voluntarily giving up control.
   */
  public class PauseException extends StallException
  {
  }

  /**
   * Indicates execution has stalled due to resource starvation. Execution
   * can resume when the associated future completes.
   */
  public class WaitException extends StallException
  {
    // TODO: fill in details.
  }

  /**
   * Return the next row (item) of data. Throws {@link EofException}
   * at EOF. This structure avoids the need for a "has next" check,
   * streamlining tight inner loops.
   */
  Batch next() throws StallException;
}
