/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletTestUtils;
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

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.OPENAPI_SPEC_PATH;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.requestParameterFor;

public class RequestParameterTest {
  private Map<String, CruiseControlParameters> _endpointToClass;
  private OpenAPI _openAPI;
  /**
   * Specify endpoints to be tested
   */
  @Before
  public void setupParameterClasses() throws Exception {
    _endpointToClass = new HashMap<>();
    KafkaCruiseControlConfig defaultConfig = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    String webserverApiUrlPrefix = KafkaCruiseControlServletTestUtils.getDefaultWebServerApiUrlPrefix();
    for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.cachedValues()) {
      _endpointToClass.put((webserverApiUrlPrefix + endpoint.toString()).toLowerCase(),
                           ((CruiseControlParameters) (defaultConfig.getClass(requestParameterFor(endpoint).parametersClass()).newInstance())));
    }
  }

  /**
   * Load the OpenAPI files for endpoints and compares them against the source code
   */
  @Test
  public void checkOpenApiSpec() {
    OpenAPIV3Parser openApiParser = new OpenAPIV3Parser();
    ParseOptions options = new ParseOptions();
    options.setResolveFully(true);
    options.setFlatten(true);

    SwaggerParseResult parseResult = openApiParser.readLocation(OPENAPI_SPEC_PATH, null, options);
    _openAPI = parseResult.getOpenAPI();
    Map<String, Set<String>> schema = parseSchema();
    Assert.assertEquals(schema.keySet(), _endpointToClass.keySet());
    for (Map.Entry<String, Set<String>> endpoint: schema.entrySet()) {
      Assert.assertTrue(_endpointToClass.containsKey(endpoint.getKey()));
      CruiseControlParameters endpointParams = _endpointToClass.get(endpoint.getKey());
      Assert.assertEquals(endpoint.getValue(), endpointParams.caseInsensitiveParameterNames());
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
      Assert.assertFalse(parameterSet.contains(parameter.getName()));
      parameterSet.add(parameter.getName());
    }
    return parameterSet;
  }

  /**
   * parse the entire schema defined in yaml file and return a map
   *
   * @return a map of endpoints to its corresponding set of parameters
   */
  public Map<String, Set<String>> parseSchema() {
    Map<String, Set<String>> parsedSchema = new HashMap<>();
    Paths paths = _openAPI.getPaths();
    for (Map.Entry<String, PathItem> endpoint : paths.entrySet()) {
      parsedSchema.put(endpoint.getKey(), parseEndpoint(endpoint.getValue()));
    }
    return parsedSchema;
  }
}
