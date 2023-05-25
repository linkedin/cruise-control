import argparse
from typing import Any, Callable, Union

import pytest

from cruisecontrolclient.client import ExecutionContext
from cruisecontrolclient.client.CCParameter.CommaSeparatedParameter import BrokerIdParameter


@pytest.mark.parametrize('endpoint', ExecutionContext.NAME_TO_ENDPOINT.keys())
def test_check(endpoint: str,
               namespace_builder: Union[Callable[[Any], argparse.Namespace], Callable[[Any, Any], argparse.Namespace]],
               capsys):
    if BrokerIdParameter not in ExecutionContext.NAME_TO_ENDPOINT[endpoint].available_Parameters:
        with pytest.raises(SystemExit):
            namespace_builder(endpoint, '123')
        captured = capsys.readouterr()

        assert "unrecognized arguments" in captured.err
    else:
        with pytest.raises(SystemExit):
            namespace_builder(endpoint)

        captured = capsys.readouterr()

        assert "the following arguments are required: brokers" in captured.err
