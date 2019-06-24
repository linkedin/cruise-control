#!/usr/bin/env python3

# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# To be able to easily parse command-line arguments
import argparse

# To be able to make more precise type hints
from typing import Dict, Tuple, Type  # noqa

# Convenience functions for displaying the retrieved Response
from cruisecontrolclient.client.Display import display_response

# To be able to instantiate Endpoint objects
import cruisecontrolclient.client.Endpoint as Endpoint

# To be able to make requests and get a response given a URL to cruise-control
from cruisecontrolclient.client.Responder import JSONDisplayingResponderGet, JSONDisplayingResponderPost

# To be able to compose a URL to hand to requests
from cruisecontrolclient.client.Query import generate_url_from_cc_socket_address


def add_add_parameter_argument(p: argparse.ArgumentParser):
    """
    This should be used with all cruise-control endpoint parsers, to provide
    forward compatibility and greater operational flexibility.
    :param p:
    :return:
    """
    p.add_argument('--add-parameter', '--add-parameters', metavar='PARAM=VALUE',
                   help="Manually specify one or more parameters and their value in the cruise-control endpoint, like 'param=value'",
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
                   help="Manually remove one or more parameters from the cruise-control endpoint, like 'param'",
                   nargs='+')


# Define the argparse flags that cannot be used for endpoint parameters
#
# This helps the multi-stage argument parsing not to conflict with itself
# during the different stages.
non_parameter_flags = {
    'add_parameter',
    'endpoint_subparser',
    'socket_address',
    'remove_parameter',
}

# Define the in-order available endpoints for programmatically building the argparse CLI
available_endpoints: Tuple[Type[Endpoint.AbstractEndpoint]] = (
    Endpoint.AddBrokerEndpoint,
    Endpoint.AdminEndpoint,
    Endpoint.BootstrapEndpoint,
    Endpoint.DemoteBrokerEndpoint,
    Endpoint.FixOfflineReplicasEndpoint,
    Endpoint.KafkaClusterStateEndpoint,
    Endpoint.LoadEndpoint,
    Endpoint.PartitionLoadEndpoint,
    Endpoint.PauseSamplingEndpoint,
    Endpoint.ProposalsEndpoint,
    Endpoint.RebalanceEndpoint,
    Endpoint.RemoveBrokerEndpoint,
    Endpoint.ResumeSamplingEndpoint,
    Endpoint.StateEndpoint,
    Endpoint.StopProposalExecutionEndpoint,
    Endpoint.TrainEndpoint,
    Endpoint.UserTasksEndpoint
)

# A mapping from the names of the subparsers to the Endpoint object they should instantiate.
#
# Like:
# {"add_broker": AddBrokerEndpoint,
#  "add_brokers": AddBrokerEndpoint,
#  "admin": AdminEndpoint,
#  "bootstrap": BootstrapEndpoint,
#  "demote_broker": DemoteBrokerEndpoint,
#  "demote_brokers": DemoteBrokerEndpoint,
#  "fix_offline_replicas": FixOfflineReplicasEndpoint,
#  "kafka_cluster_state": KafkaClusterStateEndpoint,
#  "load": LoadEndpoint,
#  "partition_load": PartitionLoadEndpoint,
#  "pause_sampling": PauseSamplingEndpoint,
#  "proposals": ProposalsEndpoint,
#  "rebalance": RebalanceEndpoint,
#  "remove_broker": RemoveBrokerEndpoint,
#  "remove_brokers": RemoveBrokerEndpoint,
#  "resume_sampling": ResumeSamplingEndpoint,
#  "state": StateEndpoint,
#  "stop_proposal_execution": StopProposalExecutionEndpoint,
#  "stop": StopProposalExecutionEndpoint,
#  "train": TrainEndpoint,
#  "user_task": UserTasksEndpoint,
#  "user_tasks": UserTasksEndpoint}
dest_to_Endpoint: Dict[str, Type[Endpoint.AbstractEndpoint]] = {}
available_parameter_set = set()
for endpoint in available_endpoints:
    dest_to_Endpoint[endpoint.name] = endpoint
    if 'aliases' in endpoint.argparse_properties['kwargs']:
        endpoint_aliases = endpoint.argparse_properties['kwargs']['aliases']
        for alias in endpoint_aliases:
            dest_to_Endpoint[alias] = endpoint
    for parameter in endpoint.available_Parameters:
        available_parameter_set.add(parameter)

# A mapping from the names of the argparse parameters to their cruise-control parameter name
#
# Like:
# {'allow_capacity_estimation': 'allow_capacity_estimation',
#  'brokers': 'brokerid',
#  'clearmetrics': 'clearmetrics',
#  'client_id': 'client_ids',
#  'concurrency': 'concurrent_partition_movements_per_broker',
#  'data_from': 'data_from',
#  'disable_self_healing_for': 'disable_self_healing_for',
#  'dry_run': 'dryRun',
#  'enable_self_healing_for': 'enable_self_healing_for',
#  'end_timestamp': 'end',
#  'endpoint': 'endpoints',
#  'exclude_follower_demotion': 'exclude_follower_demotion',
#  'exclude_recently_demoted_brokers': 'exclude_recently_demoted_brokers',
#  'exclude_recently_removed_brokers': 'exclude_recently_removed_brokers',
#  'excluded_topics': 'excluded_topics',
#  'goals': 'goals',
#  'ignore_proposal_cache': 'ignore_proposal_cache',
#  'leader_concurrency': 'concurrent_leader_movements',
#  'max_load': 'max_load',
#  'min_valid_partition_ratio': 'min_valid_partition_ratio',
#  'number_of_entries_to_show': 'entries',
#  'partition': 'partition',
#  'reason': 'reason',
#  'resource': 'resource',
#  'skip_hard_goal_check': 'skip_hard_goal_check',
#  'skip_urp_demotion': 'skip_urp_demotion',
#  'start_timestamp': 'start',
#  'strategies': 'replica_movement_strategies',
#  'substate': 'substates',
#  'super_verbose': 'super_verbose',
#  'text': 'json',
#  'throttle_removed_broker': 'throttle_removed_broker',
#  'timestamp': 'time',
#  'topic': 'topic',
#  'types': 'types',
#  'use_ready_default_goals': 'use_ready_default_goals',
#  'user_task_ids': 'user_task_ids',
#  'verbose': 'verbose'}
flag_to_parameter_name: Dict[str, str] = {}
for parameter in available_parameter_set:
    argparse_parameter_name = None
    # argparse names this parameter's flag after the first string in 'args'
    for possible_argparse_name in parameter.argparse_properties['args']:
        # argparse chooses flag names only from the --flags
        if not possible_argparse_name.startswith('-'):
            argparse_parameter_name = possible_argparse_name.replace('-', '_')
        elif not possible_argparse_name.startswith('--'):
            continue
        else:
            argparse_parameter_name = possible_argparse_name.lstrip('-').replace('-', '_')
            break
    if argparse_parameter_name and argparse_parameter_name in flag_to_parameter_name:
        raise ValueError(f"Colliding parameter flags: {argparse_parameter_name}")
    else:
        flag_to_parameter_name[argparse_parameter_name] = parameter.name


def query_cruise_control(args):
    """
    Handles asking cruise-control for a requests.Response object, and pretty-
    printing the Response.text.

    :param args:
    :return:
    """
    # Deepcopy the args so that we can delete keys from it as we handle them.
    #
    # Otherwise successive iterations will step on each other's toes.
    arg_dict = vars(args)

    # If we have a broker list, we need to make it into a comma-separated list
    # and pass it to the Endpoint at instantiation.
    if 'brokers' in arg_dict:
        comma_broker_id_list = ",".join(args.brokers)
        endpoint: Endpoint.AbstractEndpoint = dest_to_Endpoint[args.endpoint_subparser](comma_broker_id_list)
        # Prevent trying to add this parameter a second time.
        del arg_dict['brokers']

    # Otherwise we can directly instantiate the Endpoint
    else:
        endpoint: Endpoint.AbstractEndpoint = dest_to_Endpoint[args.endpoint_subparser]()

    # Iterate only over the parameter flags; warn user if conflicts exist
    for flag in arg_dict:
        if flag in non_parameter_flags:
            pass
        else:
            # Presume None is ternary for ignore
            if arg_dict[flag] is not None:
                param_name = flag_to_parameter_name[flag]
                # Check for conflicts in this endpoint's parameter-space,
                # which here probably means that the user is specifying more
                # than one irresolvable flag.
                #
                # For the StateEndpoint only, we don't care if we overwrite it.
                # This is because we presume 'substates:executor' at instantiation.
                if endpoint.has_param(param_name) and not isinstance(endpoint, Endpoint.StateEndpoint):
                    existing_value = endpoint.get_value(param_name)
                    raise ValueError(
                        f"Parameter {param_name}={existing_value} already exists in this endpoint.\n"
                        f"Unclear whether it's safe to remap to {param_name}={arg_dict[flag]}")
                else:
                    # If we have a destination broker list, we need to make it into a comma-separated list
                    if flag == 'destination_broker':
                        comma_broker_id_list = ",".join(arg_dict[flag])
                        endpoint.add_param(param_name, comma_broker_id_list)
                    else:
                        endpoint.add_param(param_name, arg_dict[flag])

    # We added this parameter already; don't attempt to add it again
    if 'destination_broker' in arg_dict:
        del arg_dict['destination_broker']

    # Handle hacking in json=true, if the user hasn't specified
    #
    # This is because it is easier to know when a JSON response is final,
    # compared to a text response.
    #
    # In fact, because it is not possible programmatically to know when a
    # text response is final, Responder actually does not support text responses.
    if not endpoint.has_param('json'):
        endpoint.add_param('json', 'true')

    # Handle add-parameter and remove-parameter flags
    #
    # Handle deconflicting adding and removing parameters, but don't
    # warn the user if they're overwriting an existing flag, since
    # these flags are meant as an admin-mode workaround to well-meaning defaults
    adding_parameter = 'add_parameter' in arg_dict and arg_dict['add_parameter']
    if adding_parameter:
        # Build a dictionary of parameters to add
        parameters_to_add = {}

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

    # The 'remove_parameter' string may not be in our namespace, and even if it
    # is, there may be no parameters supplied to it.
    #
    # Accordingly, check both conditions and store as a simpler boolean.
    removing_parameter = 'remove_parameter' in arg_dict and arg_dict['remove_parameter']
    if removing_parameter:
        # Build a set of parameters to remove
        parameters_to_remove = set()
        for item in arg_dict['remove_parameter']:
            parameters_to_remove.add(item)

    # Validate that we didn't receive ambiguous input
    if adding_parameter and removing_parameter:
        if set(parameters_to_add) & parameters_to_remove:
            raise ValueError("Parameter present in --add-parameter and in --remove-parameter; unclear how to proceed")

    # Having validated parameters, now actually add or remove them.
    #
    # Do this without checking for conflicts from existing parameter=value mappings,
    # since we presume that if the user supplied these, they really want them
    # to override existing parameter=value mappings
    if adding_parameter:
        for parameter, value in parameters_to_add.items():
            endpoint.add_param(parameter, value)
    if removing_parameter:
        for parameter in parameters_to_remove:
            endpoint.remove_param(parameter)

    # Handle instantiating the correct URL for the Responder
    url = generate_url_from_cc_socket_address(args.socket_address, endpoint)

    # Handle instantiating the correct Responder
    if endpoint.http_method == "GET":
        json_responder = JSONDisplayingResponderGet(url)
    elif endpoint.http_method == "POST":
        json_responder = JSONDisplayingResponderPost(url)
    else:
        raise ValueError("Unexpected http_method {} in endpoint".format(endpoint.http_method))

    response = json_responder.retrieve_response()
    display_response(response)


def main():
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
    for endpoint in available_endpoints:
        endpoint_parser = endpoint_subparser.add_parser(*endpoint.argparse_properties['args'],
                                                        **endpoint.argparse_properties['kwargs'])
        endpoint_to_parser_instance[endpoint.name] = endpoint_parser
        for parameter in endpoint.available_Parameters:
            endpoint_parser.add_argument(*parameter.argparse_properties['args'],
                                         **parameter.argparse_properties['kwargs'])
        # Hack in some future-proofing by allowing users to add and remove parameter=value mappings
        add_add_parameter_argument(endpoint_parser)
        add_remove_parameter_argument(endpoint_parser)

    # Parse the provided arguments, then do the desired action
    args = parser.parse_args()

    query_cruise_control(args)


if __name__ == "__main__":
    main()
