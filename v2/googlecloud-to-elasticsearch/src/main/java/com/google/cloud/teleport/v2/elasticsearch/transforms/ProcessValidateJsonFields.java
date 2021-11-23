/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ProcessValidateJsonFields is used to fix fields which contains only dot in name. */
public class ProcessValidateJsonFields extends PTransform<PCollection<String>, PCollection<String>> {

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    return input.apply(ParDo.of(new ValidateJsonFieldsFn()));
  }

  static class ValidateJsonFieldsFn extends DoFn<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateJsonFieldsFn.class);
    ObjectMapper objectMapper = new ObjectMapper();

    @ProcessElement
    public void processElement(ProcessContext context) {
      String input = context.element();
      JsonNode node = null;
      try {
        node = removeEmptyFields((ObjectNode) objectMapper.readTree(input));
      } catch (JsonProcessingException e) {
        LOG.warn("Unable to parse Json payload. " + e);
      }

      context.output(node.asText());
    }

    public static ObjectNode removeEmptyFields(final ObjectNode jsonNode) {
      ObjectNode ret = new ObjectMapper().createObjectNode();
      Iterator<Map.Entry<String, JsonNode>> iter = jsonNode.fields();

      while (iter.hasNext()) {
        Map.Entry<String, JsonNode> entry = iter.next();
        String key = entry.getKey();
        JsonNode value = entry.getValue();

        if (key.equals(".")) {
          continue;
        }

        if (value instanceof ObjectNode) {
          Map<String, ObjectNode> map = new HashMap<>();
          map.put(key, removeEmptyFields((ObjectNode)value));
          ret.setAll(map);
        }
        else if (value instanceof ArrayNode) {
          ret.set(key, removeEmptyFields((ArrayNode)value));
        }
        else if (value.asText() != null) {
          ret.set(key, value);
        }
      }

      return ret;
    }

    public static ArrayNode removeEmptyFields(ArrayNode array) {
      ArrayNode ret = new ObjectMapper().createArrayNode();
      Iterator<JsonNode> iter = array.elements();

      while (iter.hasNext()) {
        JsonNode value = iter.next();

        if (value instanceof ArrayNode) {
          ret.add(removeEmptyFields((ArrayNode)(value)));
        }
        else if (value instanceof ObjectNode) {
          ret.add(removeEmptyFields((ObjectNode)(value)));
        }
        else if (value != null){
          ret.add(value);
        }
      }

      return ret;
    }
  }
}
