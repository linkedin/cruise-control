import argparse

import pytest

from cruisecontrolclient.client.cccli import build_argument_parser


@pytest.fixture
def namespace_builder() -> argparse.Namespace:
    parser = build_argument_parser()

    def build(*additional_args):
        return parser.parse_args(["-a", "localhost", *additional_args])

    return build
