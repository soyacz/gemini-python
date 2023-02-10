from click.testing import CliRunner

from gemini_python import GeminiConfiguration, QueryMode
from gemini_python.console import run
from gemini_python.gemini_process import GeminiProcess
from gemini_python.schema import generate_schema

runner = CliRunner()


def test_can_run_gemini():
    result = runner.invoke(run, ["--duration", "1s"])
    assert result.exit_code == 0


def test_can_run_gemini_process():
    keyspace = generate_schema(seed=1234)
    GeminiProcess(
        config=GeminiConfiguration(mode=QueryMode.READ, duration=1), schema=keyspace
    ).run()
