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

package org.apache.druid.queryng.rows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapListWriter extends ListWriter<Map<String, Object>>
{
  public MapListWriter(RowSchema schema)
  {
    this(schema, Integer.MAX_VALUE);
  }

  public MapListWriter(RowSchema schema, int sizeLimit)
  {
    super(schema, sizeLimit);
    this.rowWriter = new MapWriter(schema, () -> batch.get(batch.size() - 1));
  }

  @Override
  protected Map<String, Object> createRow()
  {
    return new HashMap<>(schema.size());
  }

  @Override
  public BatchReader<List<Map<String, Object>>> toReader()
  {
    return MapListReader.of(schema, harvest());
  }
}
