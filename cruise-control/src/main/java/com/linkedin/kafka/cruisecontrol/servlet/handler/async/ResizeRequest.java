package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.ResizeRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ResizeParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.RESIZE_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

public class ResizeRequest extends AbstractAsyncRequest {
  protected ResizeParameters _parameters;

  public ResizeRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture("Resize");
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().submit(new ResizeRunnable(_asyncKafkaCruiseControl, future, _parameters));
    return future;
  }

  @Override
  public ResizeParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ResizeRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (ResizeParameters) validateNotNull(configs.get(RESIZE_PARAMETER_OBJECT_CONFIG),
        "Parameter configuration is missing from the request.");
  }
}
