# pylint: skip-file
from typing import Tuple

from gemini_python import CqlDto, GeminiConfiguration, do_nothing, OnErrorClb, OnSuccessClb
from gemini_python.middleware import Middleware, init_middlewares, run_middlewares


def on_success_clb(*args, **kwargs):
    return None


def on_err_clb(*args, **kwargs):
    return None


class DummyMiddleware(Middleware):
    def run(self, cql_dto: CqlDto) -> Tuple[OnSuccessClb, OnErrorClb]:
        return do_nothing, do_nothing


class DummyMiddlewareTwo(Middleware):
    def run(self, cql_dto: CqlDto) -> Tuple[OnSuccessClb, OnErrorClb]:
        return on_success_clb, on_err_clb


def test_middleware_can_be_init_and_run():
    config = GeminiConfiguration()
    middlewares = init_middlewares(config, [DummyMiddleware, DummyMiddlewareTwo])
    on_success_clbs, on_err_clbs = run_middlewares(
        cql_dto=CqlDto(statement="dummy"), middlewares=middlewares, error_handler=lambda x: x
    )

    # assert callbacks are in reverse order than middleware execution
    assert on_success_clbs[1] == do_nothing
    assert on_success_clbs[0] == on_success_clb
    assert on_err_clbs[1] == do_nothing
    assert on_err_clbs[0] == on_err_clb
