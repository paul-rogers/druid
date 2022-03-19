package org.apache.druid.testing2.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.druid.java.util.common.ISE;

public class ServiceInstance
{
  private final String container;
  private final String host;
  private final String tag;
  private final int containerPort;
  private final int hostPort;

  @JsonCreator
  public ServiceInstance(
      @JsonProperty("container") String container,
      @JsonProperty("host") String host,
      @JsonProperty("tag") String tag,
      @JsonProperty("containerPort") int containerPort,
      @JsonProperty("hostPort") int hostPort
  )
  {
    this.container = container;
    this.host = host;
    this.tag = tag;
    this.containerPort = containerPort;
    this.hostPort = hostPort;
  }

  @JsonProperty("container")
  @JsonInclude(Include.NON_NULL)
  public String container()
  {
    return container;
  }

  @JsonProperty("host")
  @JsonInclude(Include.NON_NULL)
  public String host()
  {
    return host;
  }

  @JsonProperty("tag")
  @JsonInclude(Include.NON_NULL)
  public String tag()
  {
    return tag;
  }

  @JsonProperty("containerPort")
  @JsonInclude(Include.NON_DEFAULT)
  public int containerPort()
  {
    return containerPort;
  }

  @JsonProperty("hostPort")
  @JsonInclude(Include.NON_DEFAULT)
  public int hostPort()
  {
    return hostPort;
  }

  public String resolveHost(String service)
  {
    return host != null ? host : service;
  }

  public String resolveContainer(String service)
  {
    return container != null ? container : service;
  }

  public int resolveContainerPort()
  {
    if (containerPort != 0) {
      return containerPort;
    }
    if (hostPort == 0) {
      throw new ISE("Must provide containerPort, hostPort or both");
    }
    return hostPort;
  }

  public int resolveHostPort()
  {
    if (hostPort != 0) {
      return hostPort;
    }
    if (containerPort == 0) {
      throw new ISE("Must provide containerPort, hostPort or both");
    }
    return containerPort;
  }
}
