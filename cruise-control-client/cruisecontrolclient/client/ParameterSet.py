from collections import MutableSet
from typing import Dict, Iterator

from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter


class ParameterSet(MutableSet):

    def __init__(self):
        self.parameters: Dict[str, AbstractParameter] = {}

    def __len__(self) -> int:
        return len(self.parameters)

    def __contains__(self, parameter: AbstractParameter) -> bool:
        return parameter.name in self.parameters

    def __iter__(self) -> Iterator[AbstractParameter]:
        yield from self.parameters.values()

    def add(self, parameter: AbstractParameter) -> None:
        self.parameters[parameter.name] = parameter

    def discard(self, parameter: AbstractParameter) -> None:
        del self.parameters[parameter.name]

