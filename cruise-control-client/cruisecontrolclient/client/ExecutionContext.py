# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# To be able to instantiate Endpoint objects
import cruisecontrolclient.client.Endpoint as Endpoint

# To be able to make more precise type hints
from typing import Dict, List, Set, Type

# Define the in-order available endpoints for programmatically building the argparse CLI
AVAILABLE_ENDPOINTS: List[Type[Endpoint.AbstractEndpoint]] = [
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
    Endpoint.RightsizeEndpoint,
    Endpoint.ReviewBoardEndpoint,
    Endpoint.ReviewEndpoint,
    Endpoint.StateEndpoint,
    Endpoint.StopProposalExecutionEndpoint,
    Endpoint.TrainEndpoint,
    Endpoint.TopicConfigurationEndpoint,
    Endpoint.UserTasksEndpoint
]

# Define the argparse flags that cannot be used for endpoint parameters
NON_PARAMETER_FLAGS: Set[str] = {
    'add_parameter',
    'endpoint_subparser',
    'remove_parameter',
    'socket_address'
}

# A mapping from the names of the subparsers to the Endpoint object they should instantiate.
#
# Like:
# {"add_broker": AddBrokerEndpoint,
#  "add_brokers": AddBrokerEndpoint,
#  "admin": AdminEndpoint,
#  "bootstrap": BootstrapEndpoint,
#  "demote_broker": DemoteBrokerEndpoint,
#   ...
#  "user_task": UserTasksEndpoint,
#  "user_tasks": UserTasksEndpoint}
NAME_TO_ENDPOINT: Dict[str, Type[Endpoint.AbstractEndpoint]] = {}
AVAILABLE_PARAMETER_SET = set()
for endpoint in AVAILABLE_ENDPOINTS:
    NAME_TO_ENDPOINT[endpoint.name] = endpoint
    if 'aliases' in endpoint.argparse_properties['kwargs']:
        endpoint_aliases = endpoint.argparse_properties['kwargs']['aliases']
        for alias in endpoint_aliases:
            NAME_TO_ENDPOINT[alias] = endpoint
    for parameter in endpoint.available_Parameters:
        AVAILABLE_PARAMETER_SET.add(parameter)

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
#   ...
#  'types': 'types',
#  'use_ready_default_goals': 'use_ready_default_goals',
#  'user_task_ids': 'user_task_ids',
#  'verbose': 'verbose'}
FLAG_TO_PARAMETER_NAME: Dict[str, str] = {}
for parameter in AVAILABLE_PARAMETER_SET:
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
    if argparse_parameter_name and argparse_parameter_name in FLAG_TO_PARAMETER_NAME:
        raise ValueError(f"Colliding parameter flags: {argparse_parameter_name}")
    else:
        FLAG_TO_PARAMETER_NAME[argparse_parameter_name] = parameter.name
