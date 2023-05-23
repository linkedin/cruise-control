import argparse
from typing import Any, Callable
import pytest

from cruisecontrolclient.client.cccli import build_argument_parser, get_endpoint
from cruisecontrolclient.client.ExecutionContext import ExecutionContext


@pytest.fixture
def context() -> ExecutionContext:
    return ExecutionContext()


@pytest.fixture
def namespace_builder(context: ExecutionContext) -> argparse.Namespace:
    parser = build_argument_parser(context)

    def build(*addl_args):
        return parser.parse_args(["-a", "localhost", *addl_args])

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


def test__get_endpoint__admin__correct(namespace_builder: Callable[[Any, Any], argparse.Namespace],
                                       context: ExecutionContext):
    namespace = namespace_builder("admin")
    endpoint = get_endpoint(namespace, context)

    assert endpoint
