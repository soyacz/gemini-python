import logging
from time import sleep, time

from gemini_python.executor import QueryExecutor
from gemini_python.limiter import Limiter
from gemini_python.query import QueryGenerator
from gemini_python.validator import GeminiValidator

logger = logging.getLogger(__name__)


def handle_exception(exception: Exception) -> None:
    logger.error(exception)


def run_gemini(
    generator: QueryGenerator,
    sut_executor: QueryExecutor,
    validator: GeminiValidator,
    limiter: Limiter,
) -> None:
    """Runs main gemini task: run queries against test db and oracle db in loop."""
    start = time()
    for statement, query_values in [next(generator) for _ in range(10000)]:
        limiter.increment()
        validate_results = validator.prepare_validation_method(statement, query_values)
        sut_executor.execute_async(
            statement,
            query_values,
            on_success=[limiter.decrement, validate_results],
            on_error=[limiter.decrement, handle_exception],
        )
    while limiter.value:
        # wait until all requests complete
        sleep(0.1)
    print(f"finished in: {time() - start:.2f} seconds")
