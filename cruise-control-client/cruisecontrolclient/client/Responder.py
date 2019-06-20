# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# Use the common Display library for showing non-final responses
from cruisecontrolclient.client.Display import display_response

# Use convenience function to redirect printing to stderr
from cruisecontrolclient.util.print import print_error

# To be able to make HTTP calls
import requests

# To allow us to make more-precise type hints
from typing import Callable, Dict  # noqa

# To be able to define a multithreaded way of interacting with cruise-control
from threading import Thread


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
        AbstractJSONDisplayingResponder.__init__(self, url, headers)
        self.session_http_method = self.session.get


class JSONDisplayingResponderPost(AbstractJSONDisplayingResponder):
    """
    This class returns a final requests.Response object from cruise-control
    where Response.text is JSON-formatted, and where the HTTP method is POST.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
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
        AbstractJSONResponderThread.__init__(self)
        self.json_responder = JSONDisplayingResponderGet(url, headers)


class JSONResponderPostThread(AbstractJSONResponderThread):
    """
    This class defines a Thread whose purpose is to communicate with cruise-control
    via HTTP POST and return a requests.Response object.
    """

    def __init__(self, url: str, headers: Dict[str, str] = None):
        AbstractJSONResponderThread.__init__(self)
        self.json_responder = JSONDisplayingResponderPost(url, headers)
