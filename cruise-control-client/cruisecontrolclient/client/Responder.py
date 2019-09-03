# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# Use the common Display library for showing non-final responses
from cruisecontrolclient.client.Display import display_response

# Use convenience function to redirect printing to stderr
from cruisecontrolclient.util.print import print_error

# To be able to use the Endpoint class to retrieve a response from cruise-control
from cruisecontrolclient.client.Endpoint import AbstractEndpoint

# To be able to more-easily retrieve the base url of cruise-control
from cruisecontrolclient.client.Query import generate_base_url_from_cc_socket_address

# To be able to make HTTP calls
import requests

# To allow us to make more-precise type hints
from typing import Callable, Dict  # noqa

# To be able to define a multithreaded way of interacting with cruise-control
from threading import Thread

# For composing a full URL to display to the humans
from urllib.parse import urlencode

# To be able to deprecate classes
import warnings


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
                    integer_semver = lambda x: [int(elem) for elem in x.split('.')]
                    cc_version = integer_semver(response.headers["Cruise-Control-Version"])
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


class AbstractResponder(object):
    """
    This abstract class provides the skeleton for returning a final requests.Response
    object from cruise-control.
    This class should not be used directly, since the HTTP method is not defined
    in this class.
    See the children of AbstractJSONResponder and of AbstractTextResponder
    for the concrete classes that use GET and POST to retrieve a response
    from cruise-control.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
        warnings.warn("This class is deprecated as of 0.2.0. "
                      "It may be removed entirely in future versions.",
                      DeprecationWarning,
                      stacklevel=2)

        self.url: str = url
        self.headers: Dict[str, str] = headers

        # Open a session in order to handle cruise-control cookies
        self.session: requests.Session = requests.Session()

        # This abstract class does not define which of the session's
        # HTTP methods should be used
        self.session_http_method: Callable[[str], requests.Response] = None

    def retrieve_response(self) -> requests.Response:
        raise NotImplementedError


class AbstractTextResponder(AbstractResponder):
    """
    This abstract class provides the skeleton for returning a final requests.Response
    object from cruise-control where request.text is text-formatted.
    This class should not be used directly, since the HTTP method is not defined
    in this class.
    TextResponderGet and TextResponderPost are the concrete classes for
    using GET and POST to retrieve a response from cruise-control.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
        warnings.warn("This class is deprecated as of 0.2.0. "
                      "It may be removed entirely in future versions.",
                      DeprecationWarning,
                      stacklevel=2)

        if 'json=true' in url:
            raise ValueError("url must not contain the \"json=true\" parameter")
        AbstractResponder.__init__(self, url, headers)

    def retrieve_response(self) -> requests.Response:
        """
        Returns a final requests.Response object from cruise-control
        where Response.text is text-formatted.
        :return: requests.Response
        """
        # There's actually not a good way at present to do this with
        # a text response from cruise-control.
        #
        # It is much easier to determine whether every JSON response is "final".
        raise NotImplementedError


class AbstractJSONResponder(AbstractResponder):
    print_to_stdout_enabled = False
    """
    This abstract class provides the skeleton for returning a final requests.Response
    object from cruise-control where request.text is JSON-formatted.
    This class should not be used directly, since the HTTP method is not defined
    in this class.
    JSONResponderGet and JSONResponderPost are the concrete classes for
    using GET and POST to retrieve a response from cruise-control.
    This class does NOT display intermediate responses to humans.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
        warnings.warn("This class is deprecated as of 0.2.0. "
                      "It may be removed entirely in future versions.",
                      DeprecationWarning,
                      stacklevel=2)

        if 'json=true' not in url:
            raise ValueError("url must contain the \"json=true\" parameter")
        AbstractResponder.__init__(self, url, headers)
        # This abstract class _still_ does not define which HTTP method should be used

    def retrieve_response(self) -> requests.Response:
        """
        Returns a final requests.Response object from cruise-control
        where Response.text is JSON-formatted.
        :return: requests.Response
        """
        # cruise-control's JSON response has a 'progress' key in it so long
        # as the response is not final.
        #
        # Once the response is final, it does not contain the 'progress' key.
        #
        # Accordingly, keep getting the response from this session and checking
        # it for the 'progress' key.
        #
        # Return the response as JSON once we get a valid JSON response that we
        # think is 'final'.

        # Alert the humans to long-running poll
        if self.print_to_stdout_enabled:
            print_error("Starting long-running poll of {}".format(self.url))

        # TODO: Session.get and Session.post can return a plethora of exceptions;
        # they should be handled here.
        response = self.session_http_method(self.url, headers=self.headers)
        while 'progress' in response.json().keys():
            if self.print_to_stdout_enabled:
                display_response(response)
            response = self.session_http_method(self.url, headers=self.headers)

        # return the requests.response object
        return response


class AbstractJSONDisplayingResponder(AbstractJSONResponder):
    print_to_stdout_enabled = True
    """
    This class displays intermediate responses to humans via stdout.
    """


class JSONDisplayingResponderGet(AbstractJSONDisplayingResponder):
    """
    This class returns a final requests.Response object from cruise-control
    where Response.text is JSON-formatted, and where the HTTP method is GET.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
        warnings.warn("This class is deprecated as of 0.2.0. "
                      "It may be removed entirely in future versions.",
                      DeprecationWarning,
                      stacklevel=2)

        AbstractJSONDisplayingResponder.__init__(self, url, headers)
        self.session_http_method = self.session.get


class JSONDisplayingResponderPost(AbstractJSONDisplayingResponder):
    """
    This class returns a final requests.Response object from cruise-control
    where Response.text is JSON-formatted, and where the HTTP method is POST.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
        warnings.warn("This class is deprecated as of 0.2.0. "
                      "It may be removed entirely in future versions.",
                      DeprecationWarning,
                      stacklevel=2)

        AbstractJSONDisplayingResponder.__init__(self, url, headers)
        self.session_http_method = self.session.post


class AbstractJSONResponderThread(Thread):
    """
    This abstract class defines a Thread whose purpose is to communicate with
    cruise-control and return a requests.Response object.
    This class should not be used directly, since the HTTP method is not defined
    in this class.
    JSONResponderGetThread and JSONResponderPostThread are the concrete classes
    for using GET and POST to retrieve a response from cruise-control.
    """

    def __init__(self):
        warnings.warn("This class is deprecated as of 0.2.0. "
                      "It may be removed entirely in future versions.",
                      DeprecationWarning,
                      stacklevel=2)

        Thread.__init__(self)
        # This abstract class does not define which concrete JSONResponder class to use
        self.json_responder = None
        self.response = None

    def run(self):
        self.response = self.json_responder.retrieve_response()

    def get_response(self):
        return self.response

    def get_json_response(self):
        return self.response.json()


class JSONResponderGetThread(AbstractJSONResponderThread):
    """
    This class defines a Thread whose purpose is to communicate with cruise-control
    via HTTP GET and return a requests.Response object.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
        warnings.warn("This class is deprecated as of 0.2.0. "
                      "It may be removed entirely in future versions.",
                      DeprecationWarning,
                      stacklevel=2)

        AbstractJSONResponderThread.__init__(self)
        self.json_responder = JSONDisplayingResponderGet(url, headers)


class JSONResponderPostThread(AbstractJSONResponderThread):
    """
    This class defines a Thread whose purpose is to communicate with cruise-control
    via HTTP POST and return a requests.Response object.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
        warnings.warn("This class is deprecated as of 0.2.0. "
                      "It may be removed entirely in future versions.",
                      DeprecationWarning,
                      stacklevel=2)

        AbstractJSONResponderThread.__init__(self)
        self.json_responder = JSONDisplayingResponderPost(url, headers)
