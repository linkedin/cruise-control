import argparse

import pytest

from cruisecontrolclient.client.ExecutionContext import ExecutionContext
from cruisecontrolclient.client.cccli import build_argument_parser


@pytest.fixture
def context() -> ExecutionContext:
    return ExecutionContext()


@pytest.fixture
def namespace_builder(context: ExecutionContext) -> argparse.Namespace:
    parser = build_argument_parser(context)

    def build(*additional_args):
        return parser.parse_args(["-a", "localhost", *additional_args])

    return build
