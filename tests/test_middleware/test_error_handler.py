from multiprocessing import Event
import pytest

from gemini_python import ValidationError, GeminiConfiguration
from gemini_python.error_handlers import base_error_handler


def func_that_raises_exception():
    raise Exception("An error occurred")


def func_that_raises_validation_error():
    raise ValidationError(["actual"], ["expected"])


def test_base_error_handler_sets_termination_event_if_fail_fast():
    config = GeminiConfiguration(fail_fast=True)
    termination_event = Event()
    error_handler = base_error_handler(config, termination_event)
    decorated_func = error_handler(func_that_raises_validation_error)
    # should not raise exception when validation error is raised
    decorated_func()
    assert termination_event.is_set()


def test_base_error_handler_logs_validation_error(caplog):
    config = GeminiConfiguration(fail_fast=True)
    termination_event = Event()
    error_handler = base_error_handler(config, termination_event)
    decorated_func = error_handler(func_that_raises_validation_error)
    decorated_func()
    assert "Validation error occurred" in caplog.text


def test_base_error_handler_logs_unhandled_exception(caplog):
    config = GeminiConfiguration(fail_fast=True)
    termination_event = Event()
    error_handler = base_error_handler(config, termination_event)
    decorated_func = error_handler(func_that_raises_exception)

    with pytest.raises(Exception):
        decorated_func()
    assert "Unhandled exception occurred" in caplog.text
    assert termination_event.is_set()
