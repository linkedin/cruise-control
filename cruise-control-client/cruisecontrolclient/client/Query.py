# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# To be able to make more-precise type hints
from cruisecontrolclient.client.Endpoint import AbstractEndpoint

# To be able to signify deprecation
import warnings


def generate_url_from_cc_socket_address(cc_socket_address: str, endpoint: AbstractEndpoint) -> str:
    """
    Given a cruise-control hostname[:port] and an Endpoint, return the correct URL
    for this cruise-control operation.

    Note that this URL _includes_ parameters.

    :param cc_socket_address: like hostname[:port], ip-address[:port]
    :param endpoint:
    :return: URL, the correct URL to perform the Endpoint's operation
             on the given cruise-control host, _including parameters_.
    """
    warnings.warn("This function is deprecated as of 0.2.0. "
                  "It may be removed entirely in future versions.",
                  DeprecationWarning,
                  stacklevel=2)
    url = f"http://{cc_socket_address}/kafkacruisecontrol/{endpoint.compose_endpoint()}"
    return url


def generate_base_url_from_cc_socket_address(cc_socket_address: str, endpoint: AbstractEndpoint) -> str:
    """
    Given a cruise-control hostname[:port] and an Endpoint, return the correct URL
    for this cruise-control operation.

    Note that this URL _excludes_ parameters.

    :param cc_socket_address: like hostname[:port], ip-address[:port]
    :param endpoint:
    :return: URL, the correct URL to perform the Endpoint's operation
             on the given cruise-control host, _excluding parameters_.
    """
    url = f"http://{cc_socket_address}/kafkacruisecontrol/{endpoint.name}"
    return url
