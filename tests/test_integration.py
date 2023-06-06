from multiprocessing import Event
from queue import Queue

from click.testing import CliRunner

from gemini_python import QueryMode, set_event_after_timeout, ProcessResult
from gemini_python.console import run
from gemini_python.gemini_process import GeminiProcess
from gemini_python.schema import generate_schema

runner = CliRunner()


def test_can_run_gemini():
    result = runner.invoke(run, ["--duration", "1s", "--drop-schema"])
    assert result.exit_code == 0


def test_can_run_gemini_process(config):
    config.mode = QueryMode.MIXED
    config.duration = 1
    config.drop_schema = True
    keyspace = generate_schema(config)
    termination_event = Event()
    set_event_after_timeout(termination_event, config.duration)
    results_queue: Queue[ProcessResult] = Queue()
    GeminiProcess(
        index=0,
        config=config,
        schema=keyspace,
        termination_event=termination_event,
        results_queue=results_queue,
    ).run()
