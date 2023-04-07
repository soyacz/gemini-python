from abc import ABC
from typing import List, Tuple, Type

from gemini_python import OnSuccessClb, OnErrorClb, CqlDto, GeminiConfiguration


class Middleware(ABC):
    """Abstract class for actions that occur before and after in callback after async request to SUT."""

    def __init__(self, config: GeminiConfiguration):
        self._gemini_config = config

    def run(self, cql_dto: CqlDto) -> Tuple[OnSuccessClb, OnErrorClb]:
        """CAUTION: own middleware state modification callbacks must be thread safe."""

    def teardown(self) -> None:
        """Executed after gemini finishes"""


def init_middlewares(
    config: GeminiConfiguration, middlewares: List[Type[Middleware]]
) -> List[Middleware]:
    """Creates instances of provided Middleware classes and returns them."""
    return [m(config=config) for m in middlewares]


def run_middlewares(
    cql_dto: CqlDto, middlewares: List[Middleware]
) -> Tuple[List[OnSuccessClb], List[OnErrorClb]]:
    """Executes middleware object's 'run' methods which return success and error callbacks for SUT async request.

    Returned callbacks are in reverse order - so first middleware callback is executed as the last one.
    This behavior is similar as used in web frameworks middleware systems."""
    success_callbacks = []
    error_callbacks = []
    for middleware in middlewares:
        on_success, on_error = middleware.run(cql_dto)
        success_callbacks.append(on_success)
        error_callbacks.append(on_error)
    return success_callbacks[::-1], error_callbacks[::-1]
