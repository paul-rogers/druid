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

/**
 * Builds the tree of "operator" profiles dynamically at run time.
 */
public interface ProfileStack
{
  /**
   * Push a profile onto the stack and mark it as a child of the
   * previously leaf operator.
   */
  void push(OperatorProfile profile);

  /**
   * Pop the leaf-most operator from the stack. The expected operator
   * is required to check stack integrity, since it is easy for the
   * stack to get out of sync when working with sequences.
   */
  void pop(OperatorProfile profile);

  /**
   * Like {@link #pop}, but for failure conditions. The state of the
   * stack is unknown: there may be additional operators still on the
   * stack if {@code failed} is {@code true}. These are quietly removed.
   * However if {@code failed} is {@code false}, then the state should
   * be normal and the normal rules are enforced.
   */
  void popSafely(OperatorProfile profile, boolean failed);

  /**
   * Marks a profile as a leaf: it is added to the current leaf,
   * but not pushed onto the stack since the given profile does
   * not have children.
   */
  void leaf(OperatorProfile profile);

  /**
   * When dynamically creating operators within a sequence, the
   * original parent operator has long since been popped. This
   * call restores that parent, but does not wire it up to the
   * current, unrelated leaf.
   */
  void restore(OperatorProfile profile);

  /**
   * Wraps the root operator in a new profile, replacing the
   * root with this new one. The stack should be empty at
   * this point.
   */
  void wrapRoot(OperatorProfile profile);

  /**
   * Returns the root operator produced by this stack. The root
   * operator is the first one pushed, and is retained even when
   * the root is popped from the stack. This allows the fragment
   * to obtain the root after all processing is complete.
   */
  OperatorProfile root();
}
