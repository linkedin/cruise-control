# Using `cruise-control-client` Directly
Like many command-line utilities, `cccli` has strengths and weaknesses.

If you need to interact with `cruise-control` outside of a command-line (for instance, in a `Python` script that does further processing, or in a `Python` application that needs to call `cruise-control`), you'll want to use `cruise-control-client` directly.
## Rebalance Example
Below is a code snippet that starts a rebalance on the `cruise-control` instance running at `someCruiseControlAddress:9090`.

This is equivalent to the `cccli` command
```bash
cccli -a someCruiseControlAddress:9090 rebalance
# Note that cccli presumes:
#  allow_capacity_estimation=False
#  json=True
#  dryrun=False
```
This will generate a `POST` request to `http://someCruiseControlAddress:9090/kafkacruisecontrol/rebalance?allow_capacity_estimation=False&dryrun=False&json=true`.
```python
from cruisecontrolclient.client.Endpoint import RebalanceEndpoint
from cruisecontrolclient.client.Responder import CruiseControlResponder

# 1) Generate or define the socket address for the desired cruise-control instance
cc_socket_address = 'someCruiseControlAddress:9090'

# 2) Select which endpoint and parameters to use
endpoint = RebalanceEndpoint()
parameters = endpoint.init_parameter_set()
parameters.add(parameter_name="allow_capacity_estimation", value=False)
parameters.add(parameter_name="json", value=True)

# 3) Instantiate a Responder
json_responder = CruiseControlResponder()

# 4) Start a long-running poll to retrieve a Requests.Response object
response = json_responder.retrieve_response_from_Endpoint(cc_socket_address, endpoint, parameters=parameters)

# 5) Process the response, likely by JSONifying it
json_response = response.json()
```

## Remove-Broker Example
Below is a code snippet that starts a broker removal on the `cruise-control` instance running at `someCruiseControlAddress:9090`.

This is equivalent to the `cccli` command
```bash
cccli -a someCruiseControlAddress:9090 remove-broker broker,ids,to,remove
# Note that cccli presumes:
#  allow_capacity_estimation=False
#  json=True
#  dryrun=False
```
This will generate a `POST` request to `http://someCruiseControlAddress:9090/kafkacruisecontrol/remove_broker?brokerid=broker%2Cids%2Cto%2Cremove&allow_capacity_estimation=False&dryrun=False&json=true
`.
```python
from cruisecontrolclient.client.Endpoint import RemoveBrokerEndpoint
from cruisecontrolclient.client.Responder import CruiseControlResponder

# 1) Generate or define the socket address for the desired cruise-control instance
cc_socket_address = 'someCruiseControlAddress:9090'

# 2) Select which endpoint and parameters to use
endpoint = RemoveBrokerEndpoint()
parameters = endpoint.init_parameter_set()

parameters.add(parameter_name="brokers", value="123,456")
parameters.add(parameter_name="allow_capacity_estimation", value=False)
parameters.add(parameter_name="json", value=True)

# 3) Instantiate a Responder
json_responder = CruiseControlResponder()

# 4) Start a long-running poll to retrieve a Requests.Response object
response = json_responder.retrieve_response_from_Endpoint(cc_socket_address, endpoint, parameters)

# 5) Process the response, likely by JSONifying it
json_response = response.json()
```
## Add-Broker Example
Below is a code snippet that starts a broker addition on the `cruise-control` instance running at `someCruiseControlAddress:9090`.

This is equivalent to the `cccli` command
```bash
cccli -a someCruiseControlAddress:9090 add-broker broker,ids,to,add
# Note that cccli presumes:
#  allow_capacity_estimation=False
#  json=True
#  dryrun=False
```
This will generate a `POST` request to `http://someCruiseControlAddress:9090/kafkacruisecontrol/add_broker?brokerid=broker%2Cids%2Cto%2Cadd&allow_capacity_estimation=False&dryrun=False&json=true`.
```python
from cruisecontrolclient.client.Endpoint import AddBrokerEndpoint
from cruisecontrolclient.client.Responder import CruiseControlResponder

# 1) Generate or define the socket address for the desired cruise-control instance
cc_socket_address = 'someCruiseControlAddress:9090'

# 2) Select which endpoint and parameters to use
endpoint = AddBrokerEndpoint()
parameters = endpoint.init_parameter_set()
parameters.add(parameter_name="brokers", value="ids,to,add")
parameters.add(parameter_name="allow_capacity_estimation", value=False)
parameters.add(parameter_name="json", value=True)

# 3) Instantiate a Responder
json_responder = CruiseControlResponder()

# 4) Start a long-running poll to retrieve a Requests.Response object
response = json_responder.retrieve_response_from_Endpoint(cc_socket_address, endpoint, parameters)

# 5) Process the response, likely by JSONifying it
json_response = response.json()
```
