# pylint: disable=no-value-for-parameter
import ipaddress
import logging
import click

from gemini_python.executor import QueryExecutorFactory
from gemini_python.gemini_process import GeminiProcess
from gemini_python.schema import generate_schema


logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)


def validate_ips(ctx: click.Context, param: click.Parameter, value: str) -> list[str]:
    # pylint: disable=unused-argument
    if value is None:
        click.echo(
            click.style(
                f"No ip's provided for {param.name}, skipping querying {param.name}", fg="yellow"
            )
        )
        return value
    ips = [ip.strip() for ip in value.split(",")]
    for ip_addr in ips:
        try:
            ipaddress.ip_address(ip_addr)
        except ValueError as exc:
            raise click.BadParameter(f"'{ip_addr}' is not valid ip address") from exc
    return ips


@click.command(context_settings={"show_default": True})
@click.option(
    "--mode",
    type=click.Choice(choices=("write", "read", "mixed"), case_sensitive=False),
    default="write",
    help="Query operation mode",
)
@click.option(
    "--oracle-cluster",
    "-o",
    type=str,
    callback=validate_ips,
    help="Comma separated host names or IPs of the oracle cluster that provides correct answers. If omitted no oracle will be used",
)
@click.option(
    "--test-cluster",
    "-t",
    type=str,
    callback=validate_ips,
    help="Comma separated host names or IPs of the test cluster that is system under test",
)
@click.option(
    "--queries-count",
    "-q",
    type=int,
    default=1000,
    help="Temporary param for limiting number of executed queries",
)
def run(
    mode: str = "write",
    test_cluster: list[str] | None = None,
    oracle_cluster: list[str] | None = None,
    queries_count: int = 1000,
) -> None:
    """Gemini is an automatic random testing tool for Scylla."""
    keyspace = generate_schema()
    sut_query_executor = QueryExecutorFactory.create_executor(test_cluster)
    oracle_query_executor = QueryExecutorFactory.create_executor(oracle_cluster)
    keyspace.drop(sut_query_executor)
    keyspace.drop(oracle_query_executor)
    keyspace.create(sut_query_executor)
    keyspace.create(oracle_query_executor)
    processes = []
    for _ in range(1):
        gemini_process = GeminiProcess(
            schema=keyspace,
            mode=mode,
            test_cluster=test_cluster,
            oracle_cluster=oracle_cluster,
            queries_count=queries_count,
        )
        gemini_process.start()
        processes.append(gemini_process)
    for gemini_process in processes:
        gemini_process.join()


if __name__ == "__main__":
    run()
