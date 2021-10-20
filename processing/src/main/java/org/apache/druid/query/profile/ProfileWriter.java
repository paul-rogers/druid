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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Simple query profile consumer that writes each profile to the
 * directory given, as "<query id>.json". A query should have a unique
 * query ID. If the ID is missing, or duplicates an existing profile,
 * then the profile is ignored.
 * <p>
 * This consumer can retrieve the list of saved profiles (sorted in
 * date order, newest first), and retrieve a given profile.
 * <p>
 * This version is "simple" because it glosses over fine points,
 * such as identifying the owner of the query to enforce security
 * rules, removing old profiles to conserve space, or using a
 * scalable storage pattern. This version is fine for everything
 * except a high-volume, production or user-facing server.
 */
public class ProfileWriter implements ProfileConsumer, ProfileAccessor
{
  private static final Logger logger = new Logger(ProfileWriter.class);

  private static final String SUFFIX = ".json";

  private final File profileDir;
  private final ObjectMapper mapper;

  public ProfileWriter(File profileDir)
  {
    this.profileDir = profileDir;
    this.mapper = new DefaultObjectMapper();
  }

  @Override
  public void emit(RootFragmentProfile profile)
  {
    String id = profile.queryId;

    // Ignore anonymous profiles
    if (Strings.isNullOrEmpty(id)) {
      return;
    }
    // TODO: Sanitize name since users can provide the query ID
    String baseName = id + ".json";
    File file = new File(profileDir, baseName);

    // Query IDs should be unique. If not, discard duplicates.
    if (file.exists()) {
      logger.info("Ignoring profile for duplicate query ID {}", id);
      return;
    }

    // There can be a race condition here if two duplicates appear
    // at the same time. But, again, query IDs should not be duplicated.
    try (Writer out = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
      mapper.writeValue(out, profile);
    }
    catch (Exception e) {
      file.delete();
      logger.error(e, "Failed to write profile file {}", file.getAbsolutePath());
    }
  }

  @VisibleForTesting
  public File getDir()
  {
    return profileDir;
  }

  @Override
  public InputStream getProfile(String queryId) throws ProfileNotFoundException
  {
    if (Strings.isNullOrEmpty(queryId)) {
      throw new ProfileNotFoundException(queryId);
    }
    if (queryId.contains("..") || queryId.contains("~")) {
      throw new ProfileNotFoundException(queryId);
    }
    String baseName = queryId + SUFFIX;
    File file = new File(profileDir, baseName);
    try {
      return new BufferedInputStream(new FileInputStream(file));
    }
    catch (IOException e) {
      throw new ProfileNotFoundException(queryId);
    }
  }

  /**
   * Profile description for this consumer: just the timestamp (taken from
   * the file timestamp, which is just after the query completion time)
   * and the query ID. This should be enough to allow a user to find
   * their query in the list returned.
   */
  @JsonPropertyOrder({"timestamp", "queryId"})
  public static class ProfileDescrip
  {
    @JsonProperty
    public String timestamp;
    @JsonProperty
    public String queryId;

    public ProfileDescrip(File file) throws IOException
    {
      String name = file.getName();
      this.queryId = name.substring(0, name.length() - SUFFIX.length());
      BasicFileAttributes attributes = Files.readAttributes(Paths.get(file.toURI()), BasicFileAttributes.class);
      FileTime fileTime = attributes.creationTime();
      DateTime date = DateTimes.utc(fileTime.toMillis());
      this.timestamp = date.toString();
    }
  }

  @Override
  public List<?> getProfiles(int limit) throws ProfileNotFoundException
  {
    List<ProfileDescrip> files = new ArrayList<>();
    for (File file : profileDir.listFiles()) {
      if (!file.getName().endsWith(SUFFIX)) {
        continue;
      }
      if (!file.canRead()) {
        continue;
      }
      try {
        files.add(new ProfileDescrip(file));
      }
      catch (IOException e) {
        // Ignore this file
      }
    }
    Comparator<ProfileDescrip> comparator = (ProfileDescrip f1, ProfileDescrip f2) -> -f1.timestamp.compareTo(f2.timestamp);
    files.sort(comparator);
    if (files.size() > limit) {
      files = files.subList(0, limit);
    }
    return files;
  }
}
