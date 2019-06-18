from typing import Union

from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter


class AbstractTimestampParameter(AbstractParameter):
    """
    For parameters that accept timestamps in milliseconds since the Epoch.
    """

    def __init__(self, value: Union[str, int]):
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        """
        Although there are many possible validations against a timestamp, none
        are satisfactorily one-size-fits-all.

        Accordingly, only type checking will be performed against the given value.
        :return:
        """
        if type(self.value) == str:
            try:
                int(self.value)
            except ValueError as e:
                raise ValueError(f"{self.value} cannot be cast to integer timestamp", e)
        elif type(self.value) != int:
            raise ValueError(f"{self.value} must be either a string or an integer")


class EndParameter(AbstractTimestampParameter):
    """start=[END_TIMESTAMP]"""
    name = 'end'
    description = 'The timestamp in milliseconds since the Epoch to use as the end'
    argparse_properties = {
        'args': ('--end-timestamp',),
        'kwargs': dict(help=description, metavar='END_TIMESTAMP')
    }


class StartParameter(AbstractTimestampParameter):
    """start=[START_TIMESTAMP]"""
    name = 'start'
    description = 'The timestamp in milliseconds since the Epoch to use as the start'
    argparse_properties = {
        'args': ('--start-timestamp',),
        'kwargs': dict(help=description, metavar='START_TIMESTAMP')
    }


class TimeParameter(AbstractTimestampParameter):
    """time=[TIMESTAMP]"""
    name = 'time'
    description = 'The timestamp in milliseconds since the Epoch to use'
    argparse_properties = {
        'args': ('--timestamp',),
        'kwargs': dict(help=description, metavar='TIMESTAMP')
    }
