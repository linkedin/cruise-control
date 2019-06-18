from typing import Set, Union, Collection  # noqa

# To allow for warnings that don't break the program.
#
# Specifically, we don't want this client to break forward compatibility with
# new cruise-control features and parameter choices.
#
# Accordingly, for enums, we want to warn, but not raise an Exception, when
# a provided value is outside of that enum
from warnings import warn

from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter


class AbstractSetOfChoicesParameter(AbstractParameter):
    lowercase_set_of_choices: Set[str] = set()

    def __init__(self, value: Union[str, Collection[str]]):
        if isinstance(value, Collection) and type(value) != str:
            value = ','.join(value)
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if type(self.value) == str:
            for i in self.value.split(','):
                # Assume that cruise-control uses case-insensitive enums
                if i.lower() not in self.lowercase_set_of_choices:
                    warn(f"{i} is not in the valid set of choices")
        else:
            raise ValueError(f"{self.value} must be either a string or a Collection")


class DisableSelfHealingForParameter(AbstractSetOfChoicesParameter):
    """disable_self_healing_for=[Set-of-{@link AnomalyType}]"""
    lowercase_set_of_choices = {'goal_violation', 'metric_anomaly', 'broker_failure'}
    name = 'disable_self_healing_for'
    description = "The anomaly detectors for which to disable self-healing"
    argparse_properties = {
        'args': ('--disable-self-healing-for', '--disable-self-healing'),
        # nargs='+' allows for multiple of the set to be specified
        'kwargs': dict(help=description, metavar='ANOMALY_DETECTOR', choices=lowercase_set_of_choices, nargs='+')

    }


class EnableSelfHealingForParameter(AbstractSetOfChoicesParameter):
    """enable_self_healing_for=[Set-of-{@link AnomalyType}]"""
    lowercase_set_of_choices = {'goal_violation', 'metric_anomaly', 'broker_failure'}
    name = 'enable_self_healing_for'
    description = "The anomaly detectors for which to enable self-healing"
    argparse_properties = {
        'args': ('--enable-self-healing-for', '--enable-self-healing'),
        # nargs='+' allows for multiple of the set to be specified
        'kwargs': dict(help=description, metavar='ANOMALY_DETECTOR', choices=lowercase_set_of_choices, nargs='+')
    }


class ResourceParameter(AbstractSetOfChoicesParameter):
    """resource=[RESOURCE]"""
    lowercase_set_of_choices = {"cpu", "networkinbound", "networkoutbound", "disk"}
    name = 'resource'
    description = "The host and broker-level resource by which to sort the cruise-control response"
    argparse_properties = {
        'args': ('--resource',),
        'kwargs': dict(help=description, metavar='RESOURCE', choices=lowercase_set_of_choices)
    }


class SubstatesParameter(AbstractSetOfChoicesParameter):
    """substates=[SUBSTATES]"""
    lowercase_set_of_choices = {'executor', 'analyzer', 'monitor', 'anomaly_detector'}
    name = 'substates'
    description = "The substates for which to retrieve state from cruise-control"
    argparse_properties = {
        'args': ('--substate', '--substates'),
        # nargs='+' allows for multiple of the set to be specified
        'kwargs': dict(help=description, metavar='SUBSTATE', choices=lowercase_set_of_choices, nargs='+')
    }
