import argparse
from typing import Any, Callable
import pytest

from cruisecontrolclient.client.cccli import build_argument_parser, get_endpoint
from cruisecontrolclient.client.ExecutionContext import ExecutionContext
from cruisecontrolclient.client.Endpoint import AbstractEndpoint
from cruisecontrolclient.client import CCParameter


@pytest.fixture
def context() -> ExecutionContext:
    return ExecutionContext()


@pytest.fixture
def namespace_builder(context: ExecutionContext) -> argparse.Namespace:
    parser = build_argument_parser(context)

    def build(*additional_args):
        return parser.parse_args(["-a", "localhost", *additional_args])

    return build


def test__get_endpoint__add_broker(namespace_builder: Callable[[Any, Any], argparse.Namespace],
                                   context: ExecutionContext):
    # TODO: set up parameterized fixture that passes add'l params into namespace
    namespace = namespace_builder("add_broker", "123,456")

    endpoint = get_endpoint(namespace, context)
    assert endpoint.parameter_name_to_instantiated_Parameters['brokerid'].value == '123,456'


def test__get_endpoint__admin__sysexit(namespace_builder: Callable[[Any, Any], argparse.Namespace],
                                       context: ExecutionContext, capsys):
    with pytest.raises(SystemExit) as excinfo:
        namespace_builder("admin", "123,456")

    assert excinfo.value.code == 2
    captured = capsys.readouterr()

    assert "unrecognized arguments" in captured.err


def test__get_endpoint__admin__correct(namespace_builder: Callable[[Any], argparse.Namespace],
                                       context: ExecutionContext):
    namespace = namespace_builder("admin")
    endpoint = get_endpoint(namespace, context)

    assert endpoint


def test__get_endpoint__more_than_one_flag(
        namespace_builder: Callable[[Any, Any, Any, Any], argparse.Namespace],
        mocker):
    class NewEndpoint(AbstractEndpoint):
        name = "new"
        description = "Just new endpoint"
        available_Parameters = (
            CCParameter.ReasonParameter)

        argparse_properties = {
            'args': (name,),
            'kwargs': dict(aliases=['new'], help=description)
        }

        def __init__(self):
            AbstractEndpoint.__init__(self)
            self.add_param("reason", "because I can")  # By providing param in __init__

    mock_context = mocker.MagicMock()
    mock_context.available_endpoints = (NewEndpoint,)
    mock_context.dest_to_Endpoint['new'] = NewEndpoint
    mock_context.flag_to_parameter_name = {'socket_address': None,
                                           'add_parameter': None,
                                           'endpoint_subparser': None,
                                           'remove_parameter': None
                                           }

    parser = build_argument_parser(mock_context)

    # then overriding with command-line arg, conflict arises:
    namespace = parser.parse_args(["-a", "localhost", NewEndpoint.name,
                                   "--reason", "because i want to"])

    with pytest.raises(ValueError) as e:
        get_endpoint(namespace, mock_context)

    assert "already exists in this endpoint" in e.value.args[0]


def test__get_endpoint__add_parameter__no_equals(
        namespace_builder: Callable[[Any, Any, Any, Any], argparse.Namespace],
        context: ExecutionContext):
    namespace = namespace_builder("add_broker", "123", "--add-parameter", "eggs")
    with pytest.raises(ValueError) as e:
        get_endpoint(namespace, context)

    assert "Expected \"=\" in the given parameter" in e.value.args[0]


def test__get_endpoint__add_parameter__missing_param(
        namespace_builder: Callable[[Any, Any, Any, Any], argparse.Namespace],
        context: ExecutionContext):
    namespace = namespace_builder("add_broker", "123", "--add-parameter", "eggs=")
    with pytest.raises(ValueError) as e:
        get_endpoint(namespace, context)

    assert "Expected value after \"=\" in the given parameter" in e.value.args[0]


def test__get_endpoint__add_parameter__too_many_equals(
        namespace_builder: Callable[[Any, Any, Any, Any], argparse.Namespace],
        context: ExecutionContext):
    namespace = namespace_builder("add_broker", "123", "--add-parameter", "eggs=bacon=good")
    with pytest.raises(ValueError) as e:
        get_endpoint(namespace, context)

    assert "Expected only one \"=\" in the given parameter" in e.value.args[0]


def test__get_endpoint__remove_and_add_parameter(
        namespace_builder: Callable[[Any, Any, Any, Any, Any, Any], argparse.Namespace],
        context: ExecutionContext):
    namespace = namespace_builder("add_broker", "123", "--add-parameter", "eggs=true", "--remove-parameter", "eggs")
    with pytest.raises(ValueError) as e:
        get_endpoint(namespace, context)

    assert "Parameter present in --add-parameter and in --remove-parameter; unclear how to proceed" in e.value.args[0]
