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

import java.io.InputStream;
import java.util.List;

/**
 * Optional interface implemented by the {@link ProfileConsumer} if that
 * consumer allows retrieval of the consumed profiles.
 */
public interface ProfileAccessor
{
  /**
   * Exception thrown if the requested profile is not available.
   */
  @SuppressWarnings("serial")
  public static class ProfileNotFoundException extends Exception
  {
    public ProfileNotFoundException(String queryId) {
      super("Profile not found for query ID: " + queryId);
    }
  }
  
  /**
   * Retrieves the profile given the query ID. Returns the profile as an
   * <code>InputStream</code> to be returned to the HTTP client requesting
   * the profile. Avoids the need to deserialize the JSON, which is important
   * since the profile could be written by an earlier version of the software.
   * 
   * @param queryId query ID of the profile to return
   * @return an input stream to read the profile as JSON text
   * @throws ProfileNotFoundException if no profile is available for the
   * query
   */
  InputStream getProfile(String queryId) throws ProfileNotFoundException;
  
  /**
   * Returns a list of query profiles as a set of JSON serializable 
   * objects. The objects should at least include the query ID, but could
   * include other information depending on the capabilities of the
   * consumer.
   * 
   * @param limit maximum number of profiles to return
   * @return a list of profile descriptions
   * @throws ProfileNotFoundException if this consumer can't return a
   * list of profiles for any reason
   */
  List<?> getProfiles(int limit) throws ProfileNotFoundException;
}
