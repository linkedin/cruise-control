/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.writeResponseToOutputStream;
import static javax.servlet.http.HttpServletResponse.SC_OK;


public abstract class AbstractCruiseControlResponse implements CruiseControlResponse {
  protected String _cachedResponse;
  protected KafkaCruiseControlConfig _config;

  /**
   * Note if the response corresponds to a request which is initiated by Cruise Control, the config passed in will be null.
   */
  public AbstractCruiseControlResponse(KafkaCruiseControlConfig config) {
    _cachedResponse = null;
    _config = config;
  }

  protected abstract void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters);

  @Override
  public void writeSuccessResponse(CruiseControlParameters parameters, HttpServletResponse response) throws IOException {
    boolean json = parameters.json();
    boolean wantResponseSchema = parameters.wantResponseSchema();
    discardIrrelevantResponse(parameters);
    if (json && wantResponseSchema) {
      String schema = getJsonSchema();
      writeResponseToOutputStream(response, SC_OK, json, schema, _cachedResponse, _config);
    } else {
      writeResponseToOutputStream(response, SC_OK, json, _cachedResponse, _config);
    }
  }

  @Override
  public synchronized void discardIrrelevantResponse(CruiseControlParameters parameters) {
    if (_cachedResponse == null) {
      discardIrrelevantAndCacheRelevant(parameters);
      if (_cachedResponse == null) {
        throw new IllegalStateException("Failed to cache the relevant response.");
      }
    }
  }

  @Override
  public String cachedResponse() {
    return _cachedResponse;
  }

  private String getJsonSchema() {
    JsonElement response = new JsonParser().parse(_cachedResponse);
    String schema = convertNodeToStringSchemaNode(response, null);
    return schema;
  }

  private static String convertNodeToStringSchemaNode(
          JsonElement node, String key) {
    StringBuilder result = new StringBuilder();

    if (key != null) {
      result.append("\"" + key + "\": { \"type\": \"");
    } else {
      result.append("{ \"type\": \"");
    }
    if (node.isJsonArray()) {
      result.append("array\", \"items\": [");
      JsonArray arr = node.getAsJsonArray();
      for (int i = 0; i < arr.size(); i++) {
        node = arr.get(i);
        result.append(convertNodeToStringSchemaNode(node, null));
        if (i != arr.size() - 1) {
          result.append(",");
        }
      }
      result.append("]}");
    } else if (node.isJsonPrimitive()) {
      if (node.getAsJsonPrimitive().isBoolean()) {
        result.append("boolean\" }");
      } else if (node.getAsJsonPrimitive().isNumber()) {
        result.append("number\" }");
      } else if (node.getAsJsonPrimitive().isString()) {
        result.append("string\" }");
      }
    } else if (node.isJsonObject()) {
      result.append("object\", \"properties\": ");
      result.append("{");
      for (Iterator<Map.Entry<String, JsonElement>> iterator = node.getAsJsonObject().entrySet().iterator(); iterator.hasNext(); ) {
        Map.Entry<String, JsonElement> entry = iterator.next();
        key = entry.getKey();
        JsonElement child = entry.getValue();

        result.append(convertNodeToStringSchemaNode(child, key));
        if (iterator.hasNext()) {
          result.append(",");
        }
      }
      result.append("}}");
    } else if (node.isJsonNull()) {
      result.append("}");
    }

    return result.toString();
  }
}
