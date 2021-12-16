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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Populates the Guice injector with properties read from a properties file.
 * Operates in two "modes": custom and standard.
 *
 * <h4>Standard mode</h4>
 *
 * In standard mode, the caller provides a list of optional files to load.
 * The files are assumed to appear on the class path. For example, for a
 * Historical node, the class path may contain:
 * <p><ul>
 * <li>$DRUID_HOME/conf/druid/single-server/micro-quickstart/historical</li>
 * <li> $DRUID_HOME/conf/druid/single-server/micro-quickstart/_common</li>
 * </ul>
 * <p>
 * On disk, the directory structure is, for example:
 * <pre><code>
 * $DRUID_HOME/conf/druid/single-server/micro-quickstart
 * |- _common
 * |  |- common.runtime.properties
 * |- historical
 *    |- runtime.properties</code></pre>
 * <p>
 * Finally, the list of properties files would be:
 * <ul>
 * <li>runtime.properties</li>
 * <li>common.runtime.properties</li>
 * </ul>
 * <p>
 * Now, at runtime, we search the class path for <code>common.runtime.properties</code>
 * and find it in the <code>_common</code> directory. We find the service-specific
 * <code>runtime.properties</code> in the <code>historical</code> directory.
 * <p>
 * Not all commands require configuration, so the code allows property files
 * to be missing.
 *
 * <h4>Custom mode</h4>
 *
 * In custom mode, the user can specify their own desired properties file. This
 * file replaces the standard files, even if they exist on the path. In this case,
 * the user launches Druid with the <code>-Ddruid.properties.file=&lt;name></code>
 * property set. In this case, the file <i>must</i> exist since the user named
 * it explicitly.
 */
public class StartupPropertiesModule implements Module
{
  private static final Logger log = new Logger(StartupPropertiesModule.class);
  public static final String PROPERTIES_FILE = "druid.properties.file";
  public static final String COMMON_PROPERTIES = "common.runtime.properties";
  public static final String SERVICE_PROPERTIES = "runtime.properties";

  private final String customFile;
  private boolean hasCommon;
  private boolean hasSpecific;
  private Properties props;

  public StartupPropertiesModule()
  {
    // If the druid.properties.file is set, it must point to an existing
    // properties file which we use in place of those provided in the list.
    Properties systemProps = System.getProperties();
    if (systemProps.contains(PROPERTIES_FILE)) {
      Object propsFile = systemProps.get(PROPERTIES_FILE);
      if (!(propsFile instanceof String)) {
        throw new RuntimeException(
            StringUtils.format("%s property is set but is not a string", PROPERTIES_FILE));
      }
      this.customFile = (String) propsFile;
    } else {
      this.customFile = null;
    }
  }

  @Override
  public void configure(Binder binder)
  {
    final Properties fileProps;
    if (customFile != null) {
      fileProps = loadCustomProps();
    } else {
      fileProps = loadStandardProps();
    }

    // Final properties are system properties with user properties
    // as defaults.
    props = new Properties(fileProps);
    props.putAll(System.getProperties());
    binder.bind(Properties.class).toInstance(props);
  }

  public boolean hasConfig()
  {
    return customFile != null || hasCommon || hasSpecific;
  }

  private Properties loadCustomProps()
  {
    final Properties fileProps = new Properties();
    try (InputStreamReader in =
        new InputStreamReader(
            new BufferedInputStream(
                new FileInputStream(new File(customFile))),
            StandardCharsets.UTF_8)) {
      log.debug("Loading properties from %s", customFile);
      fileProps.load(in);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return fileProps;
  }

  private Properties loadStandardProps()
  {
    final Properties fileProps = new Properties();
    hasCommon = loadFile(fileProps, COMMON_PROPERTIES);
    hasSpecific = loadFile(fileProps, SERVICE_PROPERTIES);
    return fileProps;
  }

  private boolean loadFile(Properties fileProps, String propertiesFile)
  {
    try (InputStream stream = ClassLoader.getSystemResourceAsStream(propertiesFile)) {
      // Allow files to be missing. This may be OK for some Druid
      // commands.
      // TODO: Ensure files were loaded if a service is run.
      if (stream == null) {
        return false;
      }
      log.debug("Loading properties from %s", propertiesFile);
      try (final InputStreamReader in = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
        fileProps.load(in);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  /**
   * Return the "config dir": the one that contains _common as a subdirectory,
   * and thus presumably contains subdirectories for other services as well.
   *
   * @return the config directory, or null if not available
   */
  public File configDir()
  {
    if (!hasCommon) {
      return null;
    }
    URL url = ClassLoader.getSystemResource(COMMON_PROPERTIES);
    File commonDir = FileUtils.toFile(url);
    if (commonDir == null) {
      return null;
    }
    return commonDir.getParentFile();
  }

  public File getSpecificConfig(String serviceName)
  {
    File configDir = configDir();
    if (configDir == null) {
      return configDir;
    }
    File serviceDir = new File(configDir, serviceName);
    if (!serviceDir.exists()) {
      return null;
    }
    File configFile = new File(serviceDir, SERVICE_PROPERTIES);
    if (!configFile.exists()) {
      return null;
    }
    return configFile;
  }

  public String specificConfigDir()
  {
    if (!hasSpecific) {
      return null;
    }
    URL url = ClassLoader.getSystemResource(SERVICE_PROPERTIES);
    File serviceDir = FileUtils.toFile(url);
    if (serviceDir == null) {
      return null;
    }
    return serviceDir.getName();
  }

  public ServicePropertiesModule serviceProperties(String serviceName)
  {
    return new ServicePropertiesModule(this, serviceName);
  }

  /**
   * Loads properties for a specific service, assuming that Druid is running
   * in multi-service mode and that the normal property directory structure
   * is available. Does nothing otherwise, which assumes that the startup
   * properties are all we have.
   *
   */
  public static class ServicePropertiesModule implements Module
  {
    private final StartupPropertiesModule startupModule;
    private final String serviceName;

    public ServicePropertiesModule(final StartupPropertiesModule startupModule, final String serviceName)
    {
      this.startupModule = startupModule;
      this.serviceName = serviceName;
    }

    @Override
    public void configure(Binder binder)
    {
      File servicePropsFile = startupModule.getSpecificConfig(serviceName);
      if (servicePropsFile == null) {
        binder.bind(Properties.class).toInstance(startupModule.props);
        return;
      }
      String specificName = startupModule.specificConfigDir();
      if (specificName != null && !specificName.equals(serviceName)) {
        throw new RuntimeException(
            StringUtils.format(
                "In bundled mode, when running service %s, do not include service %s on the class path",
                serviceName, specificName));
      }
      final Properties fileProps = new Properties(startupModule.props);
      try (InputStreamReader in =
          new InputStreamReader(
              new BufferedInputStream(
                  new FileInputStream(servicePropsFile)))) {
        log.debug("Loading properties from %s", servicePropsFile.getName());
        fileProps.load(in);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      binder.bind(Properties.class).toInstance(fileProps);
    }
  }
}
