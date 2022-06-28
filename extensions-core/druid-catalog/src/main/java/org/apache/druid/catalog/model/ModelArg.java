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

package org.apache.druid.catalog.model;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of an table function argument which also
 * tracks if the argument was consumed.
 */
class ModelArg
{
  private final String name;
  private final Object value;
  private boolean consumed;

  protected ModelArg(String name, Object value)
  {
    this.name = name;
    this.value = value;
  }

  public static Map<String, ModelArg> convertArgs(Map<String, Object> args)
  {
    Map<String, ModelArg> argMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : args.entrySet()) {
      argMap.put(entry.getKey(), new ModelArg(entry.getKey(), entry.getValue()));
    }
    return argMap;
  }

  public Object value()
  {
    return value;
  }

  protected void consume()
  {
    if (consumed) {
      throw new IAE(StringUtils.format(
          "Argument %s is ambiguous: it is mapped to multiple JSON paths",
          name));
    }
    consumed = true;
  }

  @VisibleForTesting
  protected boolean isConsumed()
  {
    return consumed;
  }

  protected void assertConsumed()
  {
    if (!consumed) {
      throw new IAE(StringUtils.format(
          "Undefined staging function argument: %s",
          name));
    }
  }

  public String asString()
  {
    if (!(value instanceof String)) {
      throw new IAE(StringUtils.format("Argument %s must be of type VARCHAR", name));
    }
    return (String) value;
  }

  protected static void verifyArgs(Map<String, ModelArg> argMap)
  {
    for (ModelArg arg : argMap.values()) {
      arg.assertConsumed();
    }
  }
}
