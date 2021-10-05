# For tighter subclass inheritance
from abc import ABCMeta, abstractmethod

# To allow for precise type hinting to the humans
from typing import ClassVar, Collection, Dict, Tuple, Union


class AbstractParameter(metaclass=ABCMeta):
    """
    An abstract representation of a cruise-control parameter.

    Parameters are the key=value mappings that follow the endpoint in a URL.
    """
    # A string, like 'brokerid', that cannot be supplied at instantiation.
    #
    # Concrete child classes of Parameter are expected to supply a class-level
    # value for this.
    #
    # This property is meant to correspond to the parameters listed at
    # https://github.com/linkedin/cruise-control/tree/migrate_to_kafka_2_4/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/servlet/parameters
    name: ClassVar[str] = None
    description: ClassVar[str] = None
    # Define a convenience data structure to help in programmatically building CLIs
    argparse_properties: ClassVar[Dict[str, Union[Tuple[str], Dict[str, str]]]] = {
        'args': (),
        'kwargs': {}
    }

    def __init__(self, value: Union[bool, int, float, str] = None):
        # The value of this parameter, like '5', '123,234,345', or 'True'
        #
        # This value must be supplied at instantiation.
        #
        # Concrete child classes of Parameter can define an expected type
        # for the value property.
        self.value = value

        # Check that the supplied value is not incorrect.
        self.validate_value()

    def __hash__(self):
        # The (name, value) tuple should uniquely define a Parameter
        return hash((self.name, self.value))

    @abstractmethod
    def validate_value(self):
        """
        This method provides further validation on the supplied value.

        If the supplied value is invalid for this parameter, raises ValueError.
        """
        pass


class MinValidPartitionRatioParameter(AbstractParameter):
    """min_valid_partition_ratio=[min_valid_partition_ratio]"""
    name = 'min_valid_partition_ratio'
    description = 'The minimum required ratio of monitored topics [0-1]'
    argparse_properties = {
        'args': ('--min-valid-partition-ratio',),
        'kwargs': dict(help=description, metavar='R', type=float)
    }

    def __init__(self, value: float):
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if type(self.value) != float and type(self.value) != int:
            raise ValueError(f"{self.value} must be a float or an integer")
        if self.value < 0 or self.value > 1:
            raise ValueError(f"{self.value} must be between 0 and 1, inclusive")


class PartitionParameter(AbstractParameter):
    """partition=[partition/start_partition-end_partition]"""
    name = 'partition'
    description = "The partition or [start]-[end] partitions to return"
    argparse_properties = {
        'args': ('--partition', '--partitions'),
        'kwargs': dict(help=description, metavar='PARTITION_OR_RANGE')
    }

    def __init__(self, value: Union[int, Tuple[int, int], str]):
        if isinstance(value, Collection):
            value = '-'.join(value)
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if type(self.value) == int:
            pass
        elif type(self.value) == str:
            if len(self.value.split('-')) > 2:
                raise ValueError(f"{self.value} must contain either 1 integer, or 2 '-'-separated integers")
        else:
            raise ValueError(f"{self.value} must either be a string, an integer, or a Collection of two integers")


class ReasonParameter(AbstractParameter):
    """
    reason=[reason-for-pause]
    reason=[reason-for-request]
    reason=[reason-for-review]
    """
    name = 'reason'
    description = 'The human-readable reason for this action'
    argparse_properties = {
        'args': ('--reason',),
        'kwargs': dict(help=description, metavar='REASON', type=str)
    }

    def validate_value(self):
        """
        The validation strategy here is to avoid a Bobby Tables situation
        by ensuring that the provided reason does not contain any
        RFC-3986 "reserved" characters.

        See https://tools.ietf.org/html/rfc3986#section-2.2
        See also https://xkcd.com/327/
        :return:
        """
        gen_delims = set(":/?#[]@")
        sub_delims = set("!$&'()*+,;=")
        reserved_html_characters = gen_delims | sub_delims
        for character in self.value:
            if character in reserved_html_characters:
                raise ValueError(f"\"{character}\" is a reserved HTML character"
                                 " and cannot be transmitted correctly to cruise-control"
                                 " as part of the reason= parameter")
