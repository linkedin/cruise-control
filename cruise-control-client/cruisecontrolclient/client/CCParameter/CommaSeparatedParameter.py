from typing import Union, List

from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter


class AbstractCommaSeparatedParameter(AbstractParameter):
    def __init__(self, value: Union[str, List[str]]):
        # Transform the list into a comma-separated string to store self.value
        # in the format that is closest to what cruise-control expects
        if type(value) == list:
            value = ','.join(value)
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if type(self.value) != str:
            raise ValueError(f"{self.value} is not a string value")


class ApproveParameter(AbstractCommaSeparatedParameter):
    """approve=[id1,id2,...]"""
    name = 'approve'
    description = "The review IDs to approve"
    argparse_properties = {
        'args': ('--approve',),
        'kwargs': dict(help=description, nargs='+')
    }


class BrokerIdParameter(AbstractCommaSeparatedParameter):
    """brokerid=[id1,id2...]"""
    name = 'brokerid'
    description = 'Comma-separated and/or space-separated list of broker IDs'
    argparse_properties = {
        'args': ('brokers',),
        'kwargs': dict(help=description,
                       nargs='+')
    }


class ClientIdsParameter(AbstractCommaSeparatedParameter):
    """client_ids=[Set-of-ClientIdentity]"""
    name = 'client_ids'
    description = "The set of Client IDs by which to filter the user_tasks response"
    argparse_properties = {
        'args': ('--client-id', '--client-ids'),
        'kwargs': dict(help=description, nargs='+')
    }


class DestinationBrokerIdsParameter(AbstractCommaSeparatedParameter):
    """destination_broker_ids=[id1,id2...]"""
    name = 'destination_broker_ids'
    description = 'Comma-separated and/or space-separated list of broker IDs'
    argparse_properties = {
        'args': ('--destination-broker',
                 '--destination-brokers',
                 '--destination-broker-id',
                 '--destination-broker-ids'),
        'kwargs': dict(help=description,
                       nargs='+')
    }


class DiscardParameter(AbstractCommaSeparatedParameter):
    """discard=[id1,id2,...]"""
    name = 'discard'
    description = "The review IDs to discard"
    argparse_properties = {
        'args': ('--discard',),
        'kwargs': dict(help=description, nargs='+')
    }


class DropRecentlyDemotedBrokersParameter(AbstractCommaSeparatedParameter):
    """drop_recently_demoted_brokers=[id1,id2...]"""
    name = 'drop_recently_demoted_brokers'
    description = 'Comma-separated and/or space-separated list of broker IDs'
    argparse_properties = {
        'args': ('--drop-recently-demoted-broker',
                 '--drop-recently-demoted-brokers',
                 '--drop-recently-demoted-broker-id',
                 '--drop-recently-demoted-brokers-ids'),
        'kwargs': dict(help=description,
                       nargs='+')
    }


class DropRecentlyRemovedBrokersParameter(AbstractCommaSeparatedParameter):
    """drop_recently_removed_brokers=[id1,id2...]"""
    name = 'drop_recently_removed_brokers'
    description = 'Comma-separated and/or space-separated list of broker IDs'
    argparse_properties = {
        'args': ('--drop-recently-removed-broker',
                 '--drop-recently-removed-brokers',
                 '--drop-recently-removed-broker-id',
                 '--drop-recently-removed-brokers-ids'),
        'kwargs': dict(help=description,
                       nargs='+')
    }


class EndpointsParameter(AbstractCommaSeparatedParameter):
    """endpoints=[Set-of-{@link EndPoint}]"""
    name = 'endpoints'
    description = "The set of Endpoint by which to filter the user_tasks response"
    argparse_properties = {
        'args': ('--endpoint', '--endpoints'),
        'kwargs': dict(help=description, nargs='+')
    }


class GoalsParameter(AbstractCommaSeparatedParameter):
    """goals=[goal1,goal2...]"""
    name = 'goals'
    description = 'Comma-separated and/or space-separated ordered list of goals'
    argparse_properties = {
        'args': ('--goals',),
        'kwargs': dict(help=description, nargs='+')
    }


class ReplicaMovementStrategiesParameter(AbstractCommaSeparatedParameter):
    """replica_movement_strategies=[strategy1,strategy2...]"""
    name = 'replica_movement_strategies'
    description = 'Comma-separated and/or space-separated list of replica movement strategies'
    argparse_properties = {
        'args': ('--strategies',),
        'kwargs': dict(help=description, nargs='+')
    }


class ReviewIDsParameter(AbstractCommaSeparatedParameter):
    """review_ids=[id1,id2,...]"""
    name = 'review_ids'
    description = "The review IDs by which to filter the review_board response"
    argparse_properties = {
        'args': ('--review-ids', '--review-id', '--reviews', '--review'),
        'kwargs': dict(help=description, nargs='+')
    }


class TypesParameter(AbstractCommaSeparatedParameter):
    """types=[Set-of-{@link UserTaskManager.TaskState}]"""
    name = 'types'
    description = "The set of TaskStates by which to filter the user_tasks response"
    argparse_properties = {
        'args': ('--types', '--type', '--task-states', '--task-state'),
        'kwargs': dict(help=description, nargs='+')
    }


class UserTaskIdsParameter(AbstractCommaSeparatedParameter):
    """user_task_ids=[Set-of-USER-TASK-IDS]"""
    name = 'user_task_ids'
    description = "The set of UserTaskIDs by which to filter the user_tasks response"
    argparse_properties = {
        'args': ('--user-task-ids', '--user-task-id'),
        'kwargs': dict(help=description, nargs='+')
    }
