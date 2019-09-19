# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# Use convenience function to redirect printing to stderr
from cruisecontrolclient.util.print import print_error

# To be able to use the Endpoint class to retrieve a response from cruise-control
from cruisecontrolclient.client.Endpoint import AbstractEndpoint

# To be able to more-easily retrieve the base url of cruise-control
from cruisecontrolclient.client.Query import generate_base_url_from_cc_socket_address

# To be able to make HTTP calls
import requests

# For composing a full URL to display to the humans
from urllib.parse import urlencode

# To inform humans about possibly too-old versions of cruise-control
import warnings

# To currectly determine version
import re


class CruiseControlResponder(requests.Session):
    """
    This class is intended to lightly wrap requests' Session class,
    in order to provide the cruise-control-client with some basic
    sanity checking and session-management functionality.
    """

    def retrieve_response(self, method, url, **kwargs) -> requests.Response:
        """
        Returns a final requests.Response object from cruise-control
        where Response.text is JSON-formatted.

        :return: requests.Response
        """
        # Alert the humans that long-running request is starting
        if 'params' in kwargs:
            url_with_params = f"{url}?{urlencode(kwargs['params'])}"
        else:
            url_with_params = url
        print_error(f"Starting long-running poll of {url_with_params}")
        for key, value in kwargs.items():
            if key == 'params':
                continue
            else:
                print_error(f"{key}: {value}")

        # Convenience closure to not have to copy-paste the parameters from
        # this current environment.
        def inner_request_helper():
            return self.request(method, url, **kwargs)

        def is_response_final(response: requests.Response):
            # Define an inner convenience closure to avoid
            # repeating ourselves
            def json_or_text_guesser():
                try:
                    # Try to guess whether the JSON response is final
                    return "progress" not in response.json().keys()

                except ValueError:
                    # We have a non-JSON (probably plain text) response,
                    # and a non-202 status code.
                    #
                    # This response may not be final, but we have no
                    # way of doing further guessing, so warn the humans
                    # as best as we can, then presume finality.
                    cc_version = response.headers.get('Cruise-Control-Version')
                    if cc_version is not None:
                        warnings.warn(
                            f"json=False received from cruise-control version ({cc_version}) "
                            f"that does not support 202 response codes. "
                            f"Please upgrade cruise-control to >=2.0.61, or "
                            f"use json=True with cruise-control-client. "
                            f"Returning a potentially non-final response.")
                    # No cc_version in the response headers
                    else:
                        # cruise-control won't return version information if
                        # servlet receives too-large-URI request
                        if response.status_code == 414:
                            pass
                        else:
                            warnings.warn(
                                "Unable to determine cruise-control version. "
                                "Returning a potentially non-final response.")
                    return True

            # We're talking to a version of cruise-control that supports
            # 202: accepted, and we know that this response is not final.
            if response.status_code == 202:
                return False
            else:
                # Guess about whether this version of cruise-control supports 202
                if "Cruise-Control-Version" in response.headers:
                    # define a regex for extracting only leading digits and periods from the Cruise-Control-Version
                    non_decimal = re.compile(r'[^\d.]+')
                    integer_semver = lambda x: [int(elem) for elem in x.split('.')]
                    cc_version = integer_semver(non_decimal.sub('', response.headers["Cruise-Control-Version"]))
                    # 202 is supported and was not returned; response final
                    if cc_version >= [2, 0, 61]:
                        return True
                    # 202 is not supported and was not returned; guess further
                    else:
                        return json_or_text_guesser()
                # Probably we're getting a response (like a 414) before cruise-control
                # can decorate the headers with version information
                else:
                    return json_or_text_guesser()

        response = inner_request_helper()
        final_response = is_response_final(response)
        while not final_response:
            print_error(response.text)
            response = inner_request_helper()
            final_response = is_response_final(response)

        # return the requests.response object
        return response

    def retrieve_response_from_Endpoint(self,
                                        cc_socket_address: str,
                                        endpoint: AbstractEndpoint,
                                        **kwargs):
        """
        Returns a final requests.Response object from cruise-control
        where Response.text is JSON-formatted.

        This method is a convenience wrapper around the more-general retrieve_response.

        :return: requests.Response
        :param cc_socket_address: like someCruiseControlAddress:9090
        :param endpoint: an instance of an Endpoint
        :return:
        """
        return self.retrieve_response(
            method=endpoint.http_method,
            url=generate_base_url_from_cc_socket_address(cc_socket_address, endpoint),
            params=endpoint.get_composed_params(),
            **kwargs
        )
