import functools
import logging
from multiprocessing.synchronize import Event as EventClass
from typing import Callable, Any

from gemini_python import ValidationError, GeminiConfiguration

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# decorator that catches exceptions and if GeminiConfiguration.fail_fast is true sets termination_event
def base_error_handler(config: GeminiConfiguration, termination_event: EventClass) -> Callable:
    """Decorator that catches exceptions and if GeminiConfiguration.fail_fast is true sets termination_event.
    In case of undefined error"""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except ValidationError as exc:
                logger.error("Validation error occurred: %s", exc)
                if config.fail_fast:
                    termination_event.set()
            except Exception as exc:
                logger.error("Unhandled exception occurred: %s", exc)
                termination_event.set()
                raise exc
            return None

        return wrapper

    return decorator
