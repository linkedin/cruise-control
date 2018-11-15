/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.setResponseCode;
import static javax.servlet.http.HttpServletResponse.SC_OK;


public abstract class AbstractCruiseControlResponse implements CruiseControlResponse {
  /**
   * Return a valid JSON encoded String
   *
   * @param parameters Parameters of the requested execution.
   * @return a valid JSON encoded String
   */
  protected abstract String getJSONString(CruiseControlParameters parameters);
  /**
   * Write the Cruise Control response to the given output stream.
   *
   * @param out Output stream to write the Cruise Control response.
   * @param parameters Parameters of the requested execution.
   */
  protected abstract void writeOutputStream(OutputStream out, CruiseControlParameters parameters);

  @Override
  public void writeSuccessResponse(CruiseControlParameters parameters, HttpServletResponse response) throws IOException {
    OutputStream out = response.getOutputStream();
    boolean json = parameters.json();
    setResponseCode(response, SC_OK, json);
    if (json) {
      String jsonString = getJSONString(parameters);
      response.setContentLength(jsonString.length());
      out.write(jsonString.getBytes(StandardCharsets.UTF_8));
    } else {
      writeOutputStream(out, parameters);
    }
    out.flush();
  }
}
