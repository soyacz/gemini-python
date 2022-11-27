# pylint: disable=no-value-for-parameter
import ipaddress
import logging
import re
from typing import List

import click

from gemini_python.executor import QueryExecutorFactory
from gemini_python.gemini_process import GeminiProcess
from gemini_python.load_generator import QueryMode
from gemini_python.schema import generate_schema


logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

duration_pattern = re.compile(r"(?P<hours>[\d]*)h|(?P<minutes>[\d]*)m|(?P<seconds>[\d]*)s")


def time_period_str_to_seconds(time_period_string: str) -> int:
    """Transforms duration string into seconds int. e.g. 1h -> 3600, 1h22m->4920 or 10m->600"""
    try:
        return int(time_period_string)
    except ValueError:
        pass
    seconds = sum(
        int(g[0] or 0) * 3600 + int(g[1] or 0) * 60 + int(g[2] or 0)
        for g in duration_pattern.findall(time_period_string)
    )
    return seconds


# pylint: disable=unused-argument
def validate_time_period(ctx: click.Context, param: click.Parameter, value: str) -> int:
    try:
        seconds = time_period_str_to_seconds(value)
        if not seconds:
            raise ValueError
    except ValueError as exc:
        raise click.BadParameter(
            f"'{value}' is not valid time string for {param.name}. Example valid: '1h44m22s' or just number, e.g. '320'"
        ) from exc
    return seconds


def validate_ips(ctx: click.Context, param: click.Parameter, value: str) -> List[str] | None:
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
    type=click.Choice(tuple(member.lower() for member in QueryMode.__members__)),
    default=QueryMode.WRITE.value,
    callback=lambda c, p, v: getattr(QueryMode, v.upper()),
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
    "--drop-schema",
    type=bool,
    default=False,
    is_flag=True,
    help="Drop schema before starting tests run",
)
@click.option(
    "--duration",
    type=str,
    default="30s",
    callback=validate_time_period,
    help="duration in time format string e.g. 1h22m33s",
)
def run(
    mode: QueryMode = QueryMode.WRITE,
    test_cluster: List[str] | None = None,
    oracle_cluster: List[str] | None = None,
    duration: int = 30,
    drop_schema: bool = False,
) -> None:
    """Gemini is an automatic random testing tool for Scylla."""
    keyspace = generate_schema()
    sut_query_executor = QueryExecutorFactory.create_executor(test_cluster)
    oracle_query_executor = QueryExecutorFactory.create_executor(oracle_cluster)
    if drop_schema:
        logger.info("dropping schema %s", keyspace.name)
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
            duration=duration,
        )
        gemini_process.start()
        processes.append(gemini_process)
    for gemini_process in processes:
        gemini_process.join()


if __name__ == "__main__":
    run()
