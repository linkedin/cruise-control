# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# To be able to make more-precise type hints
from cruisecontrolclient.client.Endpoint import AbstractEndpoint


def generate_url_from_cc_socket_address(cc_socket_address: str, endpoint: AbstractEndpoint) -> str:
    """
    Given a cruise-control hostname[:port] and an Endpoint, return the correct URL
    for this cruise-control operation.

    :param cc_socket_address: like hostname[:port], ip-address[:port]
    :param endpoint:
    :return: URL, the correct URL to perform the Endpoint's operation
             on the given cruise-control host.
    """
    url = f"http://{cc_socket_address}/kafkacruisecontrol/{endpoint.compose_endpoint()}"
    return url
