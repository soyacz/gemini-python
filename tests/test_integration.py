from multiprocessing import Event

from click.testing import CliRunner

from gemini_python import QueryMode, set_event_after_timeout
from gemini_python.console import run
from gemini_python.gemini_process import GeminiProcess
from gemini_python.schema import generate_schema

runner = CliRunner()


def test_can_run_gemini():
    result = runner.invoke(run, ["--duration", "1s"])
    assert result.exit_code == 0


def test_can_run_gemini_process(config):
    config.mode = QueryMode.MIXED
    config.duration = 1
    keyspace = generate_schema(config)
    termination_event = Event()
    set_event_after_timeout(termination_event, config.duration)
    GeminiProcess(config=config, schema=keyspace, termination_event=termination_event).run()
