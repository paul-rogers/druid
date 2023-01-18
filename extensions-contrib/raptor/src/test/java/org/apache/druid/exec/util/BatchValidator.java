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

package org.apache.druid.exec.util;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.test.BatchEquality;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;

public class BatchValidator
{
  public static BatchEquality forTests()
  {
    return new BatchEquality((error, args) -> {
      Assert.fail(StringUtils.format(error, args));
    });
  }

  public static void assertEquals(Batch batch1, Batch batch2)
  {
    forTests().isEqual(batch1, batch2);
  }

  public static void assertEquals(BatchCursor cursor1, BatchCursor cursor2)
  {
    forTests().isEqual(cursor1, cursor2);
  }

  public static void assertNotEquals(Batch batch1, Batch batch2)
  {
    Assert.assertFalse(BatchEquality.simple().isEqual(batch1, batch2));
  }

  public static void assertNotEquals(BatchCursor cursor1, BatchCursor cursor2)
  {
    Assert.assertFalse(BatchEquality.simple().isEqual(cursor1, cursor2));
  }
}
