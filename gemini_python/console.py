# pylint: disable=no-value-for-parameter
import logging

import click

from gemini_python.executor import (
    CqlQueryExecutor,
    NoOpQueryExecutor,
    QueryExecutor,
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
@click.option(
    "--oracle-cluster",
    "-o",
    type=str,
    help="Comma separated host names or IPs of the oracle cluster that provides correct answers. If omitted no oracle will be used",
)
@click.option(
    "--test-cluster",
    "-t",
    type=str,
    required=True,
    help="Comma separated host names or IPs of the test cluster that is system under test",
)
def run(mode: str, test_cluster: str, oracle_cluster: str) -> None:
    """Gemini is an automatic random testing tool for Scylla."""
    keyspace = generate_schema()
    match mode:
        case "write":
            query = InsertQueryGenerator(table=keyspace.tables[0])
        case "read":
            query = SelectQueryGenerator(table=keyspace.tables[0])  # type: ignore
        case _:
            raise ValueError("Not supported query operation mode")

    sut_query_executor = CqlQueryExecutor(test_cluster.split(","))
    if oracle_cluster:
        oracle_query_executor: QueryExecutor = CqlQueryExecutor(oracle_cluster.split(","))
    else:
        oracle_query_executor = NoOpQueryExecutor()
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
