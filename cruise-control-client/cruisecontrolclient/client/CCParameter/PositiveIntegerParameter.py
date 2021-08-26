from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter


class AbstractPositiveIntegerParameter(AbstractParameter):
    def __init__(self, value: int):
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if type(self.value) != int:
            raise ValueError(f"{self.value} is not an integer value")
        elif self.value < 1:
            raise ValueError(f"{self.value} must be a positive integer")


class ConcurrentLeaderMovementsParameter(AbstractPositiveIntegerParameter):
    """concurrent_leader_movements=[POSITIVE-INTEGER]"""
    name = 'concurrent_leader_movements'
    description = 'The maximum number of concurrent leadership movements across the entire cluster'
    argparse_properties = {
        'args': ('--leader-concurrency', '--leadership-concurrency', '--concurrent-leader-movements'),
        'kwargs': dict(metavar='K', help=description, type=int)
    }


class ConcurrentPartitionMovementsPerBrokerParameter(AbstractPositiveIntegerParameter):
    """concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]"""
    name = 'concurrent_partition_movements_per_broker'
    description = 'The maximum number of concurrent partition movements per broker'
    argparse_properties = {
        'args': ('--concurrency', '--concurrent-partition-movements-per-broker'),
        'kwargs': dict(metavar='K', help=description, type=int)
    }


class DataFromParameter(AbstractPositiveIntegerParameter):
    """data_from=[valid_windows/valid_partitions]"""
    name = 'data_from'
    description = "The number of valid [windows, partitions] from which to use data"
    argparse_properties = {
        'args': ('--data-from',),
        'kwargs': dict(metavar='K', help=description, type=int)
    }


class EntriesParameter(AbstractPositiveIntegerParameter):
    """entries=[number-of-entries-to-show]"""
    name = 'entries'
    description = 'The number of entries to show in the response'
    argparse_properties = {
        'args': ('--number-of-entries-to-show', '--num-entries'),
        'kwargs': dict(metavar='K', help=description, type=int)
    }


class ReplicationFactorParameter(AbstractPositiveIntegerParameter):
    """replication_factor=[target_replication_factor]"""
    name = 'replication_factor'
    description = 'The target replication factor to which the specified topics should be set'
    argparse_properties = {
        'args': ('--replication-factor',),
        'kwargs': dict(metavar='K', help=description, type=int)
    }


class PartitionCountParameter(AbstractPositiveIntegerParameter):
    """partition_count=[target_partition_count]"""
    name = 'partition_count'
    description = 'The target partition count to which the specified topics should be set'
    argparse_properties = {
        'args': ('--partition-count',),
        'kwargs': dict(metavar='K', help=description, type=int)
    }


class NumBrokersToAddParameter(AbstractPositiveIntegerParameter):
    """num_brokers_to_add=[num_brokers_to_add]"""
    name = 'num_brokers_to_add'
    description = 'The broker count to add to the cluster'
    argparse_properties = {
        'args': ('--num-brokers-to-add',),
        'kwargs': dict(metavar='K', help=description, type=int)
    }
