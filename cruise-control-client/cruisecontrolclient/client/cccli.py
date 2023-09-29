#!/usr/bin/env python3

# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# To be able to easily parse command-line arguments
import argparse
from typing import Any, Dict, Optional, Set, Tuple, Type

# To be able to easily pass around the available endpoints and parameters
from cruisecontrolclient.client.ExecutionContext import (AVAILABLE_ENDPOINTS,
                                                         NAME_TO_ENDPOINT,
                                                         NON_PARAMETER_FLAGS,
                                                         FLAG_TO_PARAMETER_NAME)
# To be able to instantiate Endpoint objects
import cruisecontrolclient.client.Endpoint as Endpoint

# To be able to bundle the parameters sent to an endpoint
from cruisecontrolclient.client.ParameterSet import ParameterSet

# To be able to make long-running requests to cruise-control
from cruisecontrolclient.client.Responder import CruiseControlResponder


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Builds and returns an argument parser for interacting with cruise-control via CLI.

    It is expected that you can substitute another function for this function
    that returns a parser which is decorated similarly.

    :return:
    """

    # Define some inner functions that make no sense outside of this context
    def add_add_parameter_argument(p: argparse.ArgumentParser):
        """
        This should be used with all cruise-control endpoint parsers, to provide
        forward compatibility and greater operational flexibility.
        :param p:
        :return:
        """
        p.add_argument('--add-parameter', '--add-parameters', metavar='PARAM=VALUE',
                       help="Manually specify one or more parameter and its value in the cruise-control endpoint, "
                            "like 'param=value'",
                       nargs='+')

    def add_remove_parameter_argument(p: argparse.ArgumentParser):
        """
        Adds the ability to manually specify parameters to remove from the cruise-control
        endpoint, of the form 'parameter'.

        This should be used with all cruise-control endpoint parsers, to provide
        forward compatibility and greater operational flexibility.

        :param p:
        :return:
        """
        p.add_argument('--remove-parameter', '--remove-parameters', metavar='PARAM',
                       help="Manually remove one or more parameter from the cruise-control endpoint, like 'param'",
                       nargs='+')

    # Display command-line arguments for interacting with cruise-control
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--socket-address', help="The hostname[:port] of the cruise-control to interact with",
                        required=True)

    # Define subparser for the different cruise-control endpoints
    #
    # "dest" allows the name of the subparser used to be stored at args.endpoint_subparser,
    # which for some weird reason doesn't happen otherwise.
    endpoint_subparser = parser.add_subparsers(title='endpoint',
                                               description='Which cruise-control endpoint to interact with',
                                               # 'endpoint' would collide with an existing cc parameter
                                               dest='endpoint_subparser')

    # A map from endpoint names to that endpoint's argparse parser
    endpoint_to_parser_instance = {}

    # Dynamically build an argparse CLI from the Endpoint and Parameter properties
    for endpoint in AVAILABLE_ENDPOINTS:
        endpoint_parser = endpoint_subparser.add_parser(*endpoint.argparse_properties['args'],
                                                        **endpoint.argparse_properties['kwargs'])
        endpoint_to_parser_instance[endpoint.name] = endpoint_parser
        for parameter in endpoint.available_parameters:
            endpoint_parser.add_argument(*parameter.argparse_properties['args'],
                                         **parameter.argparse_properties['kwargs'])
        # Hack in some future-proofing by allowing users to add and remove parameter=value mappings
        add_add_parameter_argument(endpoint_parser)
        add_remove_parameter_argument(endpoint_parser)

    return parser


def get_endpoint_from_args(args: argparse.Namespace) -> Endpoint.AbstractEndpoint:
    return NAME_TO_ENDPOINT[args.endpoint_subparser]()


def extract_parameters_for(endpoint: Endpoint.AbstractEndpoint, args: argparse.Namespace) -> ParameterSet:
    # Use a __dict__ view of args for a more pythonic processing idiom.
    #
    # Also, shallow copy this dict, since otherwise deletions of keys from
    # this dict would have the unintended consequence of mutating `args` outside
    # the scope of this function.
    #
    # A deep copy is not needed here since in this method we're only ever
    # removing properties, not mutating the objects which those properties reference.
    arg_dict = vars(args).copy()

    parameters = set_known_parameters_for(endpoint, arg_dict)
    parameters_to_add, parameters_to_remove = handle_adhoc_parameters(arg_dict)

    # Having validated parameters, now actually add or remove them.
    #
    # Do this without checking for conflicts from existing parameter=value mappings,
    # since we presume that if the user supplied these, they really want them
    # to override existing parameter=value mappings
    for parameter, value in parameters_to_add.items():
        parameters.add((parameter, value))

    for parameter in parameters_to_remove:
        parameters.discard(parameter)

    return parameters


def set_known_parameters_for(endpoint: Endpoint, arguments: Dict[str, Any]) -> ParameterSet:
    # Iterate only over the known parameter flags
    parameters = endpoint.init_parameter_set()
    for flag in arguments:
        if flag in NON_PARAMETER_FLAGS:
            pass
        else:
            # Presume None is ternary for ignore
            value = arguments[flag]
            if value is not None:
                parameters.add(FLAG_TO_PARAMETER_NAME[flag], value)
    return parameters


def handle_adhoc_parameters(arg_dict: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Set]]:
    """
    Handles the add-parameter and remove-parameter flags that allow newer API versions to be
    supported with an earlier client.

    Handle de-conflicting adding and removing parameters, but doesn't warn the user if they're
    overwriting an existing flag, since they are meant as an admin-mode workaround to well-meaning
    defaults.
    """
    parameters_to_add = {}

    # Handle de-conflicting adding and removing parameters, but don't
    # warn the user if they're overwriting an existing flag, since
    # these flags are meant as an admin-mode workaround to well-meaning defaults
    if 'add_parameter' in arg_dict and arg_dict['add_parameter']:
        # Build a dictionary of parameters to add
        for item in arg_dict['add_parameter']:
            # Check that parameter contains an =
            if '=' not in item:
                raise ValueError("Expected \"=\" in the given parameter")

            # Check that the parameter=value string is correctly formatted
            split_item = item.split("=")
            if len(split_item) != 2:
                raise ValueError("Expected only one \"=\" in the given parameter")
            if not split_item[0]:
                raise ValueError("Expected parameter preceding \"=\"")
            if not split_item[1]:
                raise ValueError("Expected value after \"=\" in the given parameter")

            # If we are here, split_item is a correctly-formatted list of 2 items
            parameter, value = split_item
            # Add it to our running dictionary
            parameters_to_add[parameter] = value

    parameters_to_remove = set()

    # The 'remove_parameter' string may not be in our namespace, and even if it
    # is, there may be no parameters supplied to it, so check both conditions
    if 'remove_parameter' in arg_dict and arg_dict['remove_parameter']:
        # Build a set of parameters to remove
        for item in arg_dict['remove_parameter']:
            parameters_to_remove.add(item)

    if set(parameters_to_add) & parameters_to_remove:
        raise ValueError("Parameter present in --add-parameter and in --remove-parameter; "
                         "unclear how to proceed")

    return parameters_to_add, parameters_to_remove


def main():
    # Display and parse command-line arguments for interacting with cruise-control
    parser = build_argument_parser()
    args = parser.parse_args()

    # Get the endpoint that the parsed args specify
    endpoint = get_endpoint_from_args(args=args)

    # Get the parameter set to submit to the endpoint
    parameters = extract_parameters_for(endpoint, args=args)

    # Retrieve the response and display it
    response = CruiseControlResponder().retrieve_response_from_Endpoint(args.socket_address,
                                                                        endpoint=endpoint,
                                                                        parameters=parameters)
    print(response.text)


if __name__ == "__main__":
    main()
