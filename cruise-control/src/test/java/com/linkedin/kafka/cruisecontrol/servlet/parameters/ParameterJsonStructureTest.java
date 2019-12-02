/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.parser.OpenAPIV3Parser;

import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeSet;

public class ParameterJsonStructureTest {
  private Map<String, Class> _endpointToClass;
  OpenAPI _openAPI;
  /**
   * Specify endpoints to be tested
   */
  @Before
  public void setupParameterClasses() {
    _endpointToClass = new HashMap<>();
    _endpointToClass.put("/kafkacruisecontrol/rebalance", RebalanceParameters.class);
  }

  /**
   * Load the OpenAPI files for endpoints and compares them against the source code
   */
  @Test
  public void loadOpenApiSpec() throws Exception {
    OpenAPIV3Parser openApiParser = new OpenAPIV3Parser();
    String baseFileName = "../schemas/base.yaml";
    ParseOptions options = new ParseOptions();
    options.setResolve(true);
    options.setFlatten(true);

    SwaggerParseResult parseResult = openApiParser.readLocation(baseFileName, null, options);
    _openAPI = parseResult.getOpenAPI();
    Schema schema = parseSchema();
    for (Map.Entry<String, Set<String>> endpoint: schema.getEndpointsAndParameters()) {
      Assert.assertTrue(_endpointToClass.containsKey(endpoint.getKey()));
      CruiseControlParameters endpointParams = (CruiseControlParameters) (_endpointToClass.get(endpoint.getKey()).newInstance());
      Assert.assertEquals(
        endpoint.getValue(),
        endpointParams.caseInsensitiveParameterNames());
    }
  }

  /**
   * Return the list of parameters given the path item (an endpoint)
   *
   * @param pathItem Endpoint defined as a PathItem object
   * @return set of parameters for the specified endpoint
   */
  public static Set<String> parseEndpoint(PathItem pathItem) throws IllegalArgumentException {
    List<Parameter> parameterList;
    Set<String> parameterSet = new TreeSet<>();
    if (pathItem.getGet() != null) {
      parameterList = pathItem.getGet().getParameters();
    } else if (pathItem.getPost() != null) {
      parameterList = pathItem.getPost().getParameters();
    } else {
      throw new IllegalArgumentException("Schema Parser does not support HTTP methods other than GET/POST");
    }

    for (Parameter parameter : parameterList) {
      parameterSet.add(parameter.getName());
    }
    return parameterSet;
  }

  /**
   * parse the entire schema defined in yaml file and return the schema object
   *
   * @return object representing the schema defined in the yaml file
   */
  public Schema parseSchema() {
    Schema parsedSchema = new Schema();
    Paths paths = _openAPI.getPaths();
    for (Map.Entry<String, PathItem> endpoint : paths.entrySet()) {
      parsedSchema.addEndpoint(endpoint.getKey(), parseEndpoint(endpoint.getValue()));
    }
    return parsedSchema;
  }

  static class Schema {
    Map<String, Set<String>> _schema;

    /**
     * Constructor for Schema
     */
    public Schema() {
      _schema = new HashMap<>();
    }

    /**
     * Add endpoint with the set of parameters to the schema
     *
     * @param endpoint   name of the endpoint
     * @param parameters set of parameters for the endpoint
     */
    public void addEndpoint(String endpoint, Set<String> parameters) {
      _schema.put(endpoint, parameters);
    }

    /**
     * Get all the endpoints of the schema as a set of string
     */
    public Set<Map.Entry<String, Set<String>>> getEndpointsAndParameters() {
      return _schema.entrySet();
    }

    /**
     * Override toString method for pretty-printing schema
     */
    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      for (Map.Entry<String, Set<String>> endpoint : getEndpointsAndParameters()) {
        stringBuilder.append(String.format("%nEndpoint %s%n", endpoint.getKey()));
        int parameterIndex = 0;
        for (String parameter : endpoint.getValue()) {
          stringBuilder.append(String.format("parameter #%d %s%n", ++parameterIndex, parameter));
        }
      }
      return stringBuilder.toString();
    }
  }
}
