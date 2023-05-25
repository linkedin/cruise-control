from collections import MutableSet
from itertools import chain
from typing import Callable, Dict, Iterator, Optional, Set, Tuple, Type, Union

from cruisecontrolclient.client.CCParameter import AbstractParameter

primitive = Union[bool, float, int, str]


class ParameterSet(MutableSet):

    def __init__(self, allowed_parameters: Optional[Tuple[Type[AbstractParameter]]] = None):
        self._parameter_name_to_allowed_parameters: Dict[
            str, Callable[[Union[str, int, bool]], AbstractParameter]] = \
            {ap.name: ap for ap in allowed_parameters} if allowed_parameters else {}
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

    def is_allowed(self, parameter: Union[AbstractParameter, str]):
        try:
            return parameter.name in self._parameter_name_to_allowed_parameters
        except AttributeError:
            return parameter in self._parameter_name_to_allowed_parameters

    def add(self, parameter: Union[AbstractParameter, Tuple[str, primitive]]) -> None:
        try:
            key, value = parameter
        except ValueError:
            raise ValueError("Must provide two item tuple with key, value")
        except TypeError:
            if not self.is_allowed(parameter):
                raise ValueError(f"Parameter {parameter.name} not allowed within set.")
            if parameter.value is None:
                raise ValueError("Parameter has no value")
            self.instantiated_parameters[parameter.name] = parameter
            return

        if value is None:
            raise ValueError("Parameter has no value")

        if self.is_allowed(parameter):
            self.instantiated_parameters[key] = self._parameter_name_to_allowed_parameters[key](value)
        self.adhoc_parameters[key] = value

    def discard(self, parameter: Union[Type[AbstractParameter], str]) -> None:
        try:
            del self.instantiated_parameters[parameter.name]
        except AttributeError:
            if parameter in self.adhoc_parameters:
                del self.adhoc_parameters[parameter]
        except KeyError:
            pass

    def compose(self) -> Dict[str, primitive]:
        return {**self.adhoc_parameters,
                **{parameter.name: parameter.value for parameter in self.instantiated_parameters.values()}}

    def get(self, parameter: Union[Type[AbstractParameter], str]) -> Optional[Union[AbstractParameter, primitive]]:
        try:
            lookup = parameter.name
        except AttributeError:
            lookup = parameter

        try:
            return self.instantiated_parameters[lookup]
        except KeyError:
            return self.adhoc_parameters[lookup]
