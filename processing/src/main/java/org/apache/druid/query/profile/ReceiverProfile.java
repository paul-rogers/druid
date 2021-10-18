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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Operator profile node for a receiver: an operator which issues a
 * partial query over the wire, then receives the results. Implemented by
 * the {@link org.apache.druid.client.DirectDruidClient} class.
 * <p>
 * The query is "partial" because it is rewritten from the original query
 * to cover only the interval(s) available on the target data node.
 * <p>
 * The partial query is executed as a fragment by the remote node, which returns
 * a fragment profile. Since the two nodes may be of different Druid versions,
 * the fragment is received as a generic map, <i>not</i> as a deserialized
 * fragment object. That is, this is a "one way" serializable object: it is
 * to be serialized to JSON.
 * However, the receiver of the JSON may be in a server of a different version,
 * and that node should deserialize this object as a generic map.
 */
public class ReceiverProfile extends OperatorProfile
{
  // Uses the Druid-style scatter/gather terminology.
  public static final String TYPE = "gather";
  
  /**
   * The host to which the partial query was sent.
   */
  @JsonProperty
  public final String host;
  /**
   * The URL used to send the partial query.
   */
  @JsonProperty
  public final String url;
  /**
   * Whether the partial query succeeded or failed.
   */
  @JsonProperty
  public boolean succeeded;
  /**
   * If the query failed, the error message provided for the
   * failure.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String error;
  /**
   * The time, in ns., that the receiver waited until it received the first byte from
   * the data node. This is the same as the "ttbf" metric which Druid supports.
   */
  @JsonProperty
  public long firstByteNs;
  /**
   * If back-pressure is enabled, and the receiver invoked back-pressure, the
   * time, in ns., that the sender was asked to wait for the receiver.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long backPressureNs;
  /**
   * The number of rows received from the sender.
   * 
   * @See the note in {@link ChildFragmentProfile#rows} for caveats.
   */
  @JsonProperty
  public long rows;
  /**
   * The number of bytes received from the sender.
   */
  @JsonProperty
  public long bytes;
  /**
   * The response context returned from the data node.
   */
  @JsonProperty
  public Map<String, Object> response;
  /**
   * The fragment profile provided by the data node.
   */
  @JsonProperty
  public Map<String, Object> fragment;
  
  public ReceiverProfile(String host, String url)
  {
    this.host = host;
    this.url = url;
  }
}
