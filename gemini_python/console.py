import logging

import click

from gemini_python.executor import (
    CqlQueryExecutor,
)
from gemini_python.limiter import ConcurrencyLimiter
from gemini_python.query import InsertQueryGenerator, SelectQueryGenerator
from gemini_python.schema import (
    generate_schema,
)
from gemini_python.validator import GeminiValidator
from gemini_python.worker import run_gemini

logging.getLogger().addHandler(logging.StreamHandler())


@click.command(context_settings={"show_default": True})
@click.option(
    "--mode",
    type=click.Choice(choices=("write", "read", "mixed"), case_sensitive=False),
    default="read",
    help="Query operation mode",
)
def run(mode: str) -> None:
    """Gemini is an automatic random testing tool for Scylla."""
    keyspace = generate_schema()
    match mode:
        case "write":
            query = InsertQueryGenerator(table=keyspace.tables[0])
        case "read":
            query = SelectQueryGenerator(table=keyspace.tables[0])  # type: ignore
        case _:
            raise ValueError("Not supported query operation mode")

    sut_query_executor = CqlQueryExecutor(["192.168.100.3"])
    oracle_query_executor = CqlQueryExecutor(["192.168.100.2"])
    for statement in keyspace.as_cql():
        sut_query_executor.execute(statement)
        oracle_query_executor.execute(statement)
    limiter = ConcurrencyLimiter(limit=20)
    validator = GeminiValidator(oracle=oracle_query_executor)
    run_gemini(
        generator=query,
        sut_executor=sut_query_executor,
        validator=validator,
        limiter=limiter,
    )


if __name__ == "__main__":
    run()
