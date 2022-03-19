package org.apache.druid.testing2.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;

public class DruidConfig extends ServiceConfig
{

  @JsonCreator
  public DruidConfig(
      @JsonProperty("service") String service,
      @JsonProperty("instances") List<ServiceInstance> instances
  )
  {
    super(service, instances);
  }

  public String resolveUrl(String dockerHost)
  {
    return resolveUrl(dockerHost, instance());
  }

  public ServiceInstance findInstance(String instanceName)
  {
    for (ServiceInstance instance : requireInstances()) {
      if (instance.tag() != null && instance.tag().equals(instanceName)) {
        return instance;
      }
    }
    return null;
  }

  public ServiceInstance requireInstance(String instanceName) {
    ServiceInstance instance = findInstance(instanceName);
    if (instance != null) {
      return instance;
    }
    throw new ISE(
        StringUtils.format(
            "No Druid instance of service %s with name %s is defined",
            resolveService(),
            instanceName));
  }

  public String resolveUrl(String dockerHost, String instanceName)
  {
    return resolveUrl(dockerHost, requireInstance(instanceName));
  }

  public String resolveUrl(String dockerHost, ServiceInstance instance)
  {
    return StringUtils.format(
        "http://%s:%d",
        instance.resolveHost(dockerHost),
        instance.resolveHostPort());
  }

  public ServiceInstance tagOrDefault(String tag)
  {
    ServiceInstance taggedInstance = findInstance(tag);
    return taggedInstance == null ? instance() : taggedInstance;
  }
}
