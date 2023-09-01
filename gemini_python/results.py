import json
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Optional

from gemini_python import Operation


version = (Path(__file__).parent / "version.txt").read_text()


@dataclass
class ProcessResult:
    """Data Transfer Object for process result"""

    write_ops: int = 0
    write_errors: int = 0
    read_ops: int = 0
    read_errors: int = 0

    def increment_ops(self, operation: Operation) -> None:
        if operation == Operation.WRITE:
            self.write_ops += 1
        else:
            self.read_ops += 1

    def increment_errors(self, operation: Operation) -> None:
        if operation == Operation.WRITE:
            self.write_errors += 1
        else:
            self.read_errors += 1

    def __add__(self, other: "ProcessResult") -> "ProcessResult":
        if not isinstance(other, ProcessResult):
            return NotImplemented
        return ProcessResult(
            self.write_ops + other.write_ops,
            self.write_errors + other.write_errors,
            self.read_ops + other.read_ops,
            self.read_errors + other.read_errors,
        )


def process_results(results_queue: Queue[ProcessResult], outfile: Optional[Path] = None) -> bool:
    """Combine results from all gemini processes and write to file if specified.

    Returns True in case of errors found in any of the ProcessResult."""
    process_result = sum(
        [results_queue.get() for _ in range(results_queue.qsize())], ProcessResult()
    )
    result = {
        "gemini_version": version.strip(),
        "result": process_result.__dict__,
    }
    result_str = json.dumps(result, indent=2)
    if outfile is None:
        print(result_str)
    else:
        outfile.write_text(result_str)
    return bool(process_result.write_errors or process_result.read_errors)
