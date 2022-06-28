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

package org.apache.druid.metadata.catalog;

import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

public class DbUtils
{
  // Move to SqlMetadataConnector and its subclasses
  public static boolean isDuplicateRecordException(UnableToExecuteStatementException e)
  {
    // TODO(paul): Track down how to figure this out for each supported DB.

    // Derby implementation
    return e.getCause() != null &&
        e.getCause().getClass().getSimpleName().equals("DerbySQLIntegrityConstraintViolationException");
  }
}
