# pylint: disable=no-value-for-parameter
import ipaddress
import logging
import re
import sys
from datetime import timedelta
from multiprocessing import Event, Queue
from typing import List, Any, Optional

import click

from gemini_python import GeminiConfiguration, QueryMode, set_event_after_timeout, ProcessResult
from gemini_python.query_driver import QueryDriverFactory
from gemini_python.gemini_process import GeminiProcess
from gemini_python.replication_strategy import SimpleReplicationStrategy
from gemini_python.schema import generate_schema


logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


duration_pattern = re.compile(
    r"((?P<weeks>[.\d]+?)w)?"
    r"((?P<days>[.\d]+?)d)?"
    r"((?P<hours>[.\d]+?)h)?"
    r"((?P<minutes>[.\d]+?)m)?"
    r"((?P<seconds>[.\d]+?)s)?"
    r"((?P<microseconds>[.\d]+?)ms)?"
    r"((?P<milliseconds>[.\d]+?)us)?$"
)


def time_period_str_to_seconds(time_period_string: str) -> float:
    """
    Transforms duration string into seconds int. e.g. 1h -> 3600, 1h22m->4920 or 10m->600.

    :param time_period_string: A time duration string. (eg. 2h13m)
    :return float: The number of seconds in the duration.
    """
    parts = duration_pattern.match(time_period_string)
    assert parts is not None, (
        f"Could not parse any time information from '{time_period_string}'."
        " Examples of valid strings: '8h', '2d8h5m20s', '2m4s'"
    )
    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return timedelta(**time_params).total_seconds()


# pylint: disable=unused-argument
def validate_time_period(ctx: click.Context, param: click.Parameter, value: str) -> float:
    try:
        seconds = time_period_str_to_seconds(value)
        if not seconds:
            raise ValueError
    except ValueError as exc:
        raise click.BadParameter(
            f"'{value}' is not valid time string for {param.name}. Example valid: '1h44m22s' or just number, e.g. '320'"
        ) from exc
    return seconds


def validate_ips(ctx: click.Context, param: click.Parameter, value: str) -> Optional[List[str]]:
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


# Remove: "ignore_unknown_options": True, "allow_extra_args": True, when full compatibility is reached
@click.command(
    context_settings={
        "show_default": True,
        "ignore_unknown_options": True,
        "allow_extra_args": True,
    }
)
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
    default="3s",
    callback=validate_time_period,
    help="duration in time format string e.g. 1h22m33s",
)
@click.option(
    "--token-range-slices",
    type=int,
    default=10000,
    help="Number of slices to divide the token space into",
)
@click.option(
    "--concurrency",
    "-c",
    type=int,
    default=4,
    help="Number of concurrent processes",
)
@click.option("--seed", "-s", type=int, default=0, help="PRNG seed value")
@click.option("--max-tables", type=int, default=1, help="Maximum number of generated tables")
@click.option(
    "--min-clustering-keys", type=int, default=2, help="Minimum number of generated clustering keys"
)
@click.option("--min-columns", type=int, default=8, help="Minimum number of generated columns")
@click.option(
    "--min-partition-keys", type=int, default=2, help="Minimum number of generated partition keys"
)
@click.option(
    "--max-partition-keys", type=int, default=6, help="Maximum number of generated partition keys"
)
@click.option(
    "--max-clustering-keys", type=int, default=4, help="Maximum number of generated clustering keys"
)
@click.option("--max-columns", type=int, default=16, help="Maximum number of generated columns")
@click.option("--min-columns", type=int, default=8, help="Minimum number of generated columns")
@click.option("--fail-fast", "-f", is_flag=True, help="Stop on first error")
@click.option(
    "--max-mutation-retries",
    type=int,
    default=2,
    help="Maximum number of attempts to apply a mutation",
)
@click.option(
    "--max-mutation-retries-backoff",
    type=str,
    default="10ms",
    callback=validate_time_period,
    help="Duration between attempts to apply a mutation for example 10ms or 1s",
)
def run(*args: Any, **kwargs: Any) -> None:
    """Gemini is an automatic random testing tool for Scylla."""
    config = GeminiConfiguration(*args, **kwargs)
    interrupted = False
    keyspace = generate_schema(config=config)
    sut_query_driver = QueryDriverFactory.create_query_driver(config.test_cluster)
    oracle_query_driver = QueryDriverFactory.create_query_driver(config.oracle_cluster)
    if config.drop_schema and config.mode != QueryMode.READ:
        logger.info("dropping schema %s", keyspace.name)
        keyspace.drop(sut_query_driver)
        keyspace.drop(oracle_query_driver)
    keyspace.create(sut_query_driver, replication_strategy=SimpleReplicationStrategy(3))
    keyspace.create(oracle_query_driver, replication_strategy=SimpleReplicationStrategy(1))
    # drivers no longer needed in main process
    sut_query_driver.teardown()
    oracle_query_driver.teardown()
    processes = []
    termination_event = Event()
    results_queue: Queue[ProcessResult] = Queue()  # pylint: disable=unsubscriptable-object
    timer = set_event_after_timeout(termination_event, config.duration)
    for idx in range(config.concurrency):
        gemini_process = GeminiProcess(idx, config, keyspace, termination_event, results_queue)
        processes.append(gemini_process)
    for gemini_process in processes:
        gemini_process.start()
    for gemini_process in processes:
        try:
            gemini_process.join()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt, stopping...")
            termination_event.set()
            interrupted = True
    timer.cancel()
    result = sum([results_queue.get() for _ in range(results_queue.qsize())], ProcessResult())
    logger.info(result)
    if result.write_errors or result.read_errors:
        sys.exit(1)
    if interrupted:
        sys.exit(130)


if __name__ == "__main__":
    run()
