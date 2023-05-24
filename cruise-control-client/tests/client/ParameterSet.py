from cruisecontrolclient.client.ParameterSet import ParameterSet
from cruisecontrolclient.client.CCParameter.BooleanParameter import VerboseParameter, DryRunParameter


def test__parameter_set__discard__present():
    parameters = ParameterSet()
    parameters.add(VerboseParameter(True))

    parameters.discard(VerboseParameter)

    assert len(parameters) == 0


def test__parameter_set__discard__inst__not_present():
    parameters = ParameterSet()
    parameters.add(VerboseParameter(True))

    parameters.discard(DryRunParameter)

    assert len(parameters) == 1


def test__parameter_set__discard__custom_present():
    parameters = ParameterSet()
    parameters.add(('custom', 1))

    parameters.discard('custom')

    assert len(parameters) == 0


def test__parameter_set__discard__custom__not_present():
    parameters = ParameterSet()
    parameters.add(('special', 1))

    parameters.discard('custom')

    assert len(parameters) == 1
