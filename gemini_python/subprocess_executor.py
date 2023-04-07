import multiprocessing
from multiprocessing.connection import Connection
from queue import Empty, Queue
from typing import List, Iterable, Tuple

from gemini_python import CqlDto
from gemini_python.executor import QueryExecutor, QueryExecutorFactory


class QueryExecutorProcess(multiprocessing.Process):
    """Running queries in separate process.

    To avoid issues with cassandra driver that can't handle properly two different cluster connections in one process.
    """

    def __init__(
        self,
        inbound_queue: Queue[Tuple[CqlDto, Connection]],
        hosts: List[str] | None = None,
    ) -> None:
        super().__init__()
        self._cluster_ips = hosts
        self._inbound_queue = inbound_queue
        self._termination_event = multiprocessing.Event()

    def run(self) -> None:
        executor = QueryExecutorFactory.create_executor(self._cluster_ips)
        # executors must be created in run() method and not in __init__, otherwise cassandra driver hangs
        while not self._termination_event.is_set():
            try:
                cql_dto, outbound_pipe = self._inbound_queue.get(timeout=1)
                executor.execute_async(cql_dto, [outbound_pipe.send], [outbound_pipe.send])
            except Empty:
                pass
        executor.teardown()

    def stop(self) -> None:
        self._termination_event.set()
        self.join()


class SubprocessQueryExecutor(QueryExecutor):
    """Runs queries in separate subprocess.

    Creates subprocess with query executor and communicates with it via queues and pipes.
    queues/pipes generate overhead so it's first place for optimization."""

    def __init__(self, hosts: List[str] | None = None) -> None:
        self._parent_pipe, self._child_pipe = multiprocessing.Pipe()
        self._executor_queue: multiprocessing.Queue[
            Tuple[CqlDto, Connection]
        ] = multiprocessing.Queue()
        self._executor_process = QueryExecutorProcess(self._executor_queue, hosts)
        self._executor_process.start()

    def execute(self, cql_dto: CqlDto) -> Iterable | None:
        """Executes statement synchronously in executor."""
        self._executor_queue.put((cql_dto, self._child_pipe))
        return self._parent_pipe.recv()  # type: ignore

    def teardown(self) -> None:
        self._executor_process.stop()
        self._child_pipe.close()
        self._parent_pipe.close()

    def __del__(self) -> None:
        self.teardown()
