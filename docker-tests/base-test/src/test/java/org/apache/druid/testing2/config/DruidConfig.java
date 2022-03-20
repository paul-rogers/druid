package org.apache.druid.testing2.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;

public class DruidConfig extends ServiceConfig
{
  private String serviceKey;

  @JsonCreator
  public DruidConfig(
      @JsonProperty("service") String service,
      @JsonProperty("instances") List<ServiceInstance> instances
  )
  {
    super(service, instances);
  }

  protected void setServiceKey(String key)
  {
    if (serviceKey == null) {
      serviceKey = key;
    }
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

  @Override
  public String resolveService()
  {
    String resolved = super.resolveService();
    return resolved == null ? serviceKey : resolved;
  }

  public String resolveUrl(String dockerHost, String instanceName)
  {
    return resolveUrl(dockerHost, requireInstance(instanceName));
  }

  public String resolveContainerHost(ServiceInstance instance)
  {
    return instance.resolveContainerHost(resolveService());
  }

  public String resolveUrl(String dockerHost, ServiceInstance instance)
  {
    return StringUtils.format(
        "http://%s:%d",
        dockerHost,
        instance.resolveHostPort());
  }

  public ServiceInstance tagOrDefault(String tag)
  {
    ServiceInstance taggedInstance = findInstance(tag);
    return taggedInstance == null ? instance() : taggedInstance;
  }

  public String resolveContainerHost()
  {
    ServiceInstance instance = instance();
    if (instances.size() > 1) {
      throw new ISE(
          StringUtils.format("Service %s has %d hosts, default is ambiguous",
              resolveService(),
              instances.size()));
    }
    return instance.resolveContainerHost(resolveService());
  }
}
