import argparse
from typing import Any, Callable

import pytest

from cruisecontrolclient.client.cccli import get_endpoint_from_args, extract_parameters


def test__get_endpoint__add_broker(namespace_builder: Callable[[Any, Any], argparse.Namespace]):
    # TODO: set up parameterized fixture that passes add'l params into namespace
    namespace = namespace_builder("remove_broker", "123,456", "--destination-broker", "789,012", "--concurrency", "40")

    endpoint = get_endpoint_from_args(namespace)
    parameters = extract_parameters(endpoint, namespace)
    assert parameters.get('brokerid').value == '123,456'
    assert parameters.get('destination_broker_ids').value == '789,012'
    assert parameters.get('concurrent_partition_movements_per_broker').value == 40


def test__get_endpoint__admin__sysexit(namespace_builder: Callable[[Any, Any], argparse.Namespace], capsys):
    with pytest.raises(SystemExit) as excinfo:
        namespace_builder("admin", "123,456")

    assert excinfo.value.code == 2
    captured = capsys.readouterr()

    assert "unrecognized arguments" in captured.err


def test__get_endpoint__admin__correct(namespace_builder: Callable[[Any], argparse.Namespace]):
    namespace = namespace_builder("admin")
    endpoint = get_endpoint_from_args(namespace)

    assert endpoint


def test__get_endpoint__add_parameter__no_equals(
        namespace_builder: Callable[[Any, Any, Any, Any], argparse.Namespace]):
    namespace = namespace_builder("add_broker", "123", "--add-parameter", "eggs")
    endpoint = get_endpoint_from_args(namespace)
    with pytest.raises(ValueError) as e:
        extract_parameters(endpoint, namespace)

    assert "Expected \"=\" in the given parameter" in e.value.args[0]


def test__get_endpoint__add_parameter__missing_param(
        namespace_builder: Callable[[Any, Any, Any, Any], argparse.Namespace]):
    namespace = namespace_builder("add_broker", "123", "--add-parameter", "eggs=")
    endpoint = get_endpoint_from_args(namespace)
    with pytest.raises(ValueError) as e:
        extract_parameters(endpoint, namespace)

    assert "Expected value after \"=\" in the given parameter" in e.value.args[0]


def test__get_endpoint__add_parameter__too_many_equals(
        namespace_builder: Callable[[Any, Any, Any, Any], argparse.Namespace]):
    namespace = namespace_builder("add_broker", "123", "--add-parameter", "eggs=bacon=good")
    endpoint = get_endpoint_from_args(namespace)
    with pytest.raises(ValueError) as e:
        extract_parameters(endpoint, namespace)

    assert "Expected only one \"=\" in the given parameter" in e.value.args[0]


def test__get_endpoint__remove_and_add_parameter(
        namespace_builder: Callable[[Any, Any, Any, Any, Any, Any], argparse.Namespace]):
    namespace = namespace_builder("add_broker", "123", "--add-parameter", "eggs=true", "--remove-parameter", "eggs")
    endpoint = get_endpoint_from_args(namespace)
    with pytest.raises(ValueError) as e:
        extract_parameters(endpoint, namespace)

    assert "Parameter present in --add-parameter and in --remove-parameter; unclear how to proceed" in e.value.args[0]
