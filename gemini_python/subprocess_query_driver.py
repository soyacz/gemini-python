import multiprocessing
from multiprocessing.connection import Connection
from queue import Empty, Queue
from typing import List, Iterable, Tuple, Optional

from gemini_python import CqlDto
from gemini_python.query_driver import QueryDriver, QueryDriverFactory, QueryDriverException


class QueryDriverProcess(multiprocessing.Process):
    """Running queries in separate process.

    To avoid issues with cassandra driver that can't handle properly two different cluster connections in one process.
    """

    def __init__(
        self,
        inbound_queue: Queue[Tuple[CqlDto, Connection]],
        hosts: Optional[List[str]] = None,
    ) -> None:
        super().__init__()
        self._cluster_ips = hosts
        self._inbound_queue = inbound_queue
        self._termination_event = multiprocessing.Event()

    def run(self) -> None:
        query_driver = QueryDriverFactory.create_query_driver(self._cluster_ips)
        # query drivers must be created in run() method and not in __init__, otherwise cassandra driver hangs
        while not self._termination_event.is_set():
            try:
                cql_dto, outbound_pipe = self._inbound_queue.get(timeout=1)
                try:
                    result = query_driver.execute(cql_dto)
                except QueryDriverException as exc:
                    outbound_pipe.send(exc)
                    continue
                outbound_pipe.send(list(result))
            except Empty:
                pass
        query_driver.teardown()

    def stop(self) -> None:
        self._termination_event.set()
        self.join()


class SubprocessQueryDriver(QueryDriver):
    """Runs queries in separate subprocess.

    Creates subprocess with query driver and communicates with it via queues and pipes.
    queues/pipes generate overhead so it's first place for optimization."""

    def __init__(self, hosts: Optional[List[str]] = None) -> None:
        self._parent_pipe, self._child_pipe = multiprocessing.Pipe()
        self._query_driver_queue: multiprocessing.Queue[
            Tuple[CqlDto, Connection]
        ] = multiprocessing.Queue()
        self._query_driver_process = QueryDriverProcess(self._query_driver_queue, hosts)
        self._query_driver_process.start()

    def execute(self, cql_dto: CqlDto) -> Iterable:
        """Executes statement synchronously in query driver."""
        self._query_driver_queue.put((cql_dto, self._child_pipe))
        return self._parent_pipe.recv()  # type: ignore

    def teardown(self) -> None:
        self._query_driver_process.stop()
        self._child_pipe.close()
        self._parent_pipe.close()

    def __del__(self) -> None:
        self.teardown()
