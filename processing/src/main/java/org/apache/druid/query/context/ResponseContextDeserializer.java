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

package org.apache.druid.query.context;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class ResponseContextDeserializer extends StdDeserializer<ResponseContext>
{
  public ResponseContextDeserializer()
  {
    super(ResponseContext.class);
  }

  @Override
  public ResponseContext deserialize(
      final JsonParser jp,
      final DeserializationContext ctxt
  ) throws IOException
  {
    if (jp.currentToken() != JsonToken.START_OBJECT) {
      throw ctxt.wrongTokenException(jp, ResponseContext.class, JsonToken.START_OBJECT, null);
    }

    // TODO(gianm): Check if we need concurrent response context here
    final ResponseContext retVal = ResponseContext.createEmpty();

    jp.nextToken();

    while (jp.currentToken() == JsonToken.FIELD_NAME) {
      final ResponseContext.BaseKey<?> key = ResponseContext.Key.keyOf(jp.getText());

      jp.nextToken();
      final Object value = key.readValue(jp);
      retVal.add(key, value);

      jp.nextToken();
    }

    if (jp.currentToken() != JsonToken.END_OBJECT) {
      throw ctxt.wrongTokenException(jp, ResponseContext.class, JsonToken.END_OBJECT, null);
    }

    return retVal;
  }
}
