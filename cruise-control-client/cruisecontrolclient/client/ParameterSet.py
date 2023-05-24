from itertools import chain
from collections import MutableSet
from typing import Dict, Iterator, Tuple, Union

from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter

primitive = Union[bool, float, int, str]


class ParameterSet(MutableSet):

    def __init__(self):
        self.adhoc_parameters: Dict[str, primitive] = {}
        self.instantiated_parameters: Dict[str, AbstractParameter] = {}

    def __len__(self) -> int:
        return len(self.adhoc_parameters) + len(self.instantiated_parameters)

    def __contains__(self, parameter: Union[AbstractParameter, str]) -> bool:
        try:
            return parameter.name in self.instantiated_parameters
        except AttributeError:
            return parameter in self.adhoc_parameters

    def __iter__(self) -> Iterator[Union[AbstractParameter, Tuple[str, primitive]]]:
        yield from chain(self.instantiated_parameters.values(), list(self.adhoc_parameters.items()))

    def add(self, parameter: Union[AbstractParameter, Tuple[str, primitive]]) -> None:
        try:
            key, value = parameter
            self.adhoc_parameters[key] = value
        except TypeError:
            self.instantiated_parameters[parameter.name] = parameter
            assert parameter.value is not None
        except ValueError:
            raise ValueError("Must provide two item tuple with key, value")

    def discard(self, parameter: Union[AbstractParameter, str]) -> None:
        try:
            del self.instantiated_parameters[parameter.name]
        except AttributeError:
            del self.adhoc_parameters[parameter]

    def compose(self) -> Dict[str, primitive]:
        return {**self.adhoc_parameters,
                **{parameter.name: parameter.value for parameter in self.instantiated_parameters.values()}}
