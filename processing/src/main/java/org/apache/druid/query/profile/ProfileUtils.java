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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;

public class ProfileUtils
{
  private ProfileUtils()
  {
  }

  public static String classOf(Object obj)
  {
    return obj == null ? null : obj.getClass().getSimpleName();
  }

  public static boolean isSameClass(Object obj1, Object obj2)
  {
    if (obj1 == null && obj2 == null) {
      return true;
    }
    if (obj1 == null || obj2 == null) {
      return false;
    }
    return obj1.getClass() == obj2.getClass();
  }

  public static <T> Sequence<T> wrap(Sequence<T> sequence, ProfileStack profileStack, OperatorProfile profile)
  {
    return Sequences.wrap(
        sequence,
        new SequenceWrapper()
        {
          @Override
          public void before()
          {
            profileStack.restore(profile);
          }

          @Override
          public void after(boolean isDone, Throwable thrown)
          {
            profileStack.popSafely(profile, thrown != null);
          }
        });
  }
}
